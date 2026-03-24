require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const app = express();
app.use(cors());
app.use(express.json());

const API_KEY = process.env.NEXON_API_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const PORT = process.env.PORT || 3000;
const SERVERS = ['류트', '만돌린', '하프', '울프'];

const genAI = GEMINI_API_KEY ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_summary (
      id SERIAL PRIMARY KEY, target_date DATE, server_name TEXT, total_messages INT,
      peak_hour INT, popular_dungeon TEXT, horn_king_name TEXT, horn_king_count INT,
      created_at TIMESTAMP DEFAULT NOW(), UNIQUE(target_date, server_name)
    );
    CREATE TABLE IF NOT EXISTS horn (
      id SERIAL PRIMARY KEY, server_name TEXT, character_name TEXT, message TEXT,
      date_send TIMESTAMP WITH TIME ZONE, category TEXT,
      UNIQUE(server_name, character_name, message, date_send)
    );
    CREATE INDEX IF NOT EXISTS idx_date_send ON horn(date_send);
    CREATE INDEX IF NOT EXISTS idx_category ON horn(category);
    CREATE INDEX IF NOT EXISTS idx_server ON horn(server_name);
  `);
}

function classify(msg) {
  // 🔥 성장, 길드모집 등 작가님 요청 키워드 추가! (파티보다 무조건 먼저 분류됨)
  if (/길드|길원|길모|성장/.test(msg)) return 'guild';
  if (/파티|구함|모집|인원|\/\d|[0-9]\/[0-9]/.test(msg)) return 'party';
  if (/팝니다|팝|판매|삽니다|구매|구입|얼마|골드|가격/.test(msg)) return 'trade';
  return 'etc';
}

const NORMALIZE_MAP = {
  '1채': '채널1', '2채': '채널2', '브리': '브리레흐', '브레': '브리레흐',
  '1-3관': '브리1-3관', '크롬': '크롬일반', '크일': '크롬일반', '크쉬': '크롬쉬움',
  '글렌': '글렌일반', '글매': '글렌일반', '글쉬': '글렌쉬움', '풀팟': '풀파티'
};

const STOP_WORDS = new Set(['이','가','은','는','을','를','에','의','도','합니다','모집','구합니다']);

function normalizeWord(w) { return NORMALIZE_MAP[w] || w; }

async function fetchServer(serverName) {
  try {
    // 🔥 KST 기준 '오늘' 날짜를 명시적으로 계산 (넥슨이 영국 시간 기준으로 어제 데이터를 주는 현상 완벽 방지)
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    const dateStr = `${nowKST.getFullYear()}-${String(nowKST.getMonth() + 1).padStart(2, '0')}-${String(nowKST.getDate()).padStart(2, '0')}`;
    
    // 🔥 URL에 &date=오늘날짜 를 억지로 꽂아넣어 최신 데이터를 강제로 받아옵니다.
    const url = `https://open.api.nexon.com/mabinogi/v1/horn-bugle-world/history?server_name=${encodeURIComponent(serverName)}&date=${dateStr}`;
    const res = await fetch(url, { headers: { 'x-nxopen-api-key': API_KEY } });

    if (!res.ok) {
      const errData = await res.json().catch(() => ({}));
      console.error(`[${serverName}] API 오류 HTTP ${res.status}`, JSON.stringify(errData));
      return 0;
    }

    const data = await res.json();
    const items = data.horn_bugle_world_history || [];
    let newCount = 0;

    for (const item of items) {
      try {
        const result = await pool.query(
          `INSERT INTO horn (server_name, character_name, message, date_send, category)
           VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
          [serverName, item.character_name, item.message, item.date_send, classify(item.message)]
        );
        if (result.rowCount > 0) newCount++;
      } catch (e) {}
    }
    if (newCount > 0) console.log(`[${serverName}] ${newCount}건 신규 저장`);
    return newCount;
  } catch (e) {
    console.error(`[${serverName}] 오류:`, e.message);
    return 0;
  }
}

async function fetchAll() {
  for (const server of SERVERS) {
    await fetchServer(server);
    await new Promise(r => setTimeout(r, 1000)); // 429 방지용 1초 휴식
  }
}

// ── API 엔드포인트 생략 (기존 코드와 동일하게 유지) ──
// ── 실시간 피드 API (원상복구 완료!) ──
app.get('/api/feed', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const { category, keyword, server, offset } = req.query;
  
  let query = 'SELECT * FROM horn';
  const params = [];
  const conditions = [];

  if (server && server !== 'all') { params.push(server); conditions.push(`server_name = $${params.length}`); }
  if (category && category !== 'all') { params.push(category); conditions.push(`category = $${params.length}`); }
  if (keyword) { params.push(`%${keyword}%`); conditions.push(`(message ILIKE $${params.length} OR character_name ILIKE $${params.length})`); }

  if (conditions.length > 0) query += ' WHERE ' + conditions.join(' AND ');
  params.push(limit); 
  params.push(parseInt(offset) || 0);
  query += ` ORDER BY date_send DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

  try {
    const result = await pool.query(query, params);
    res.json({ items: result.rows, count: result.rows.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/stats/hourly', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  let query = `SELECT MOD(CAST(EXTRACT(HOUR FROM CAST(date_send AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Seoul') AS INTEGER), 24) as hour, COUNT(*) as count FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (req.query.server && req.query.server !== 'all') { params.push(req.query.server); query += ` AND server_name = $${params.length}`; }
  query += ' GROUP BY hour ORDER BY hour';
  const result = await pool.query(query, params);
  const hourly = new Array(24).fill(0);
  result.rows.forEach(r => { hourly[parseInt(r.hour)] = parseInt(r.count); });
  res.json({ hourly, since, days, server: req.query.server || 'all' });
});

app.get('/api/stats/category', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  let query = `SELECT category, COUNT(*) as count FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (req.query.server && req.query.server !== 'all') { params.push(req.query.server); query += ` AND server_name = $${params.length}`; }
  query += ' GROUP BY category';
  const result = await pool.query(query, params);
  const out = { party: 0, trade: 0, guild: 0, etc: 0 };
  result.rows.forEach(r => { out[r.category] = parseInt(r.count); });
  res.json(out);
});

app.get('/api/stats/summary', async (req, res) => {
  const total = await pool.query('SELECT COUNT(*) as count FROM horn');
  const today = await pool.query(`SELECT COUNT(*) as count FROM horn WHERE date_send::timestamptz >= NOW() - INTERVAL '24 hours'`);
  const serverCounts = {};
  for (const s of SERVERS) {
    const r = await pool.query('SELECT COUNT(*) as count FROM horn WHERE server_name = $1', [s]);
    serverCounts[s] = parseInt(r.rows[0].count);
  }
  res.json({ total: parseInt(total.rows[0].count), today: parseInt(today.rows[0].count), servers: serverCounts });
});

app.get('/api/stats/daily', async (req, res) => {
  const server = req.query.server;
  let query = `SELECT * FROM daily_summary ORDER BY target_date DESC, server_name ASC LIMIT 20`;
  const params = [];
  if (server && server !== 'all') { query = `SELECT * FROM daily_summary WHERE server_name = $1 ORDER BY target_date DESC LIMIT 7`; params.push(server); }
  const result = await pool.query(query, params);
  res.json(result.rows);
});

app.get('/api/stats/hall-of-fame', async (req, res) => {
  const server = req.query.server;
  let query = `
    SELECT 
      horn_king_name as name, 
      server_name, 
      COUNT(*) as win_count, 
      SUM(horn_king_count) as total_horns
    FROM daily_summary
    WHERE horn_king_name != '없음' AND horn_king_name IS NOT NULL
  `;
  const params = [];

  if (server && server !== 'all') {
    params.push(server);
    query += ` AND server_name = $1`;
  }

  query += `
    GROUP BY horn_king_name, server_name
    ORDER BY win_count DESC, total_horns DESC
    LIMIT 10
  `;

  try {
    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/stats/horn-king', async (req, res) => {
  try {
    const kings = {};
    for (const server of SERVERS) {
      // 🔥 DB 시간대 충돌을 100% 방지하는 가장 안전한 KST 날짜 비교 쿼리
      const result = await pool.query(`
        SELECT character_name, COUNT(*) as count 
        FROM horn 
        WHERE server_name = $1 
          AND (date_send AT TIME ZONE 'Asia/Seoul')::date = (NOW() AT TIME ZONE 'Asia/Seoul')::date
        GROUP BY character_name 
        ORDER BY count DESC 
        LIMIT 1
      `, [server]);
      
      if (result.rows.length > 0) {
        const name = result.rows[0].character_name;
        kings[server] = { masked: name.length <= 2 ? name[0] + 'X' : name.slice(0, 2) + 'X'.repeat(name.length - 2), count: parseInt(result.rows[0].count) };
      } else {
        kings[server] = { masked: '조용한 에린', count: 0 };
      }
    }
    res.json(kings);
  } catch (e) { res.status(500).json({ error: e.message }); }
});


// ── 길드원 모집 현황 ──
app.get('/api/stats/guilds', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const server = req.query.server;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  // 🔥 category가 guild거나, 길드 관련 키워드가 있는 메시지 모두 소환
  let query = `
    SELECT DISTINCT ON (message) character_name, message, date_send, server_name
    FROM horn
    WHERE (category = 'guild' OR message ~ '길드|길원|길모|성장') AND date_send >= $1
  `;
  const params = [since];
  if (server && server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }
  query += ` ORDER BY message, date_send DESC`;

  try {
    const result = await pool.query(query, params);
    const rows = result.rows;
    const guildMap = {};

    rows.forEach(r => {
      let guildName = null;
      // 1순위: [대괄호] 형태 길드명 추출
      const bracketMatch = r.message.match(/[\[\(【<「『](.*?)[\]\)】>」』]/);
      
      if (bracketMatch) {
        guildName = bracketMatch[1].replace(/채널\d+/, '').trim();
      } else {
        // 2순위: 'OOO에서', 'OOO 길드' 형태 추출
        const textMatch = r.message.match(/([가-힣a-zA-Z0-9]{2,10})(?:에서| 길드)/);
        if (textMatch) {
          const word = textMatch[1];
          // 🔥 '함께하실', '성장할' 처럼 '~실', '~는', '~할', '~운', '~고' 로 끝나는 동사/형용사 완벽 차단!
          // 🔥 초보, 성인, 매너 등 너무 뻔한 수식어도 차단!
          if (!/[실는할운고]$/.test(word) && !/초보|성인|매너|친목|신생|환영|모집|가입|성장|같이/.test(word)) {
            guildName = word;
          }
        }
      }

      const chMatch = r.message.match(/([0-9]+)\s*(?:채|ch)/i);
      const channel = chMatch ? chMatch[1] + '채널' : '채널 미상';

      if (guildName && guildName.length >= 2 && !/^\d+$/.test(guildName)) {
        if (!guildMap[guildName]) guildMap[guildName] = { name: guildName, messages: [], server: r.server_name };
        if (guildMap[guildName].messages.length < 3) {
          guildMap[guildName].messages.push({ text: r.message, character: r.character_name, channel: channel, date: r.date_send });
        }
      }
    });

    const guilds = Object.values(guildMap).sort((a, b) => b.messages.length - a.messages.length).slice(0, 20);
    res.json({ total: rows.length, guilds, raw: rows.slice(0, 50) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

async function generateDailySummary() {
  console.log('🌙 요약 작업 시작: 오늘의 에린 요약지 생성');
  const yesterday = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  yesterday.setDate(yesterday.getDate() - 1);
  const targetDateStr = yesterday.toISOString().split('T')[0];

  for (const server of SERVERS) {
    try {
      const hourRes = await pool.query(`SELECT EXTRACT(HOUR FROM CAST(date_send AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Seoul') as hour, COUNT(*) as count FROM horn WHERE server_name = $1 AND DATE(CAST(date_send AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Seoul') = $2 GROUP BY hour ORDER BY count DESC`, [server, targetDateStr]);
      const totalMessages = hourRes.rows.reduce((sum, r) => sum + parseInt(r.count), 0);
      const peakHour = hourRes.rows.length > 0 ? parseInt(hourRes.rows[0].hour) : 0;

      const kingRes = await pool.query(`SELECT character_name, COUNT(*) as count FROM horn WHERE server_name = $1 AND DATE(CAST(date_send AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Seoul') = $2 GROUP BY character_name ORDER BY count DESC LIMIT 1`, [server, targetDateStr]);
      const hornKingName = kingRes.rows.length > 0 ? kingRes.rows[0].character_name : '없음';
      const hornKingCount = kingRes.rows.length > 0 ? parseInt(kingRes.rows[0].count) : 0;

      await pool.query(`
        INSERT INTO daily_summary (target_date, server_name, total_messages, peak_hour, popular_dungeon, horn_king_name, horn_king_count)
        VALUES ($1, $2, $3, $4, '미정', $5, $6)
        ON CONFLICT (target_date, server_name) DO UPDATE SET
          total_messages = EXCLUDED.total_messages, peak_hour = EXCLUDED.peak_hour,
          horn_king_name = EXCLUDED.horn_king_name, horn_king_count = EXCLUDED.horn_king_count
      `, [targetDateStr, server, totalMessages, peakHour, hornKingName, hornKingCount]);
    } catch (err) { console.error(err); }
  }
}

app.get('/api/admin/force-summary', async (req, res) => {
  await generateDailySummary();
  res.send('✅ 어제 데이터 강제 정산 완료! 새로고침 해보세요.');
});

// ── 🔥 키워드 트렌드 통계 API (불용어 및 '~요', '뭉' 완벽 필터링) ──
app.get('/api/stats/keywords', async (req, res) => {
  const { server, category, days } = req.query;
  const since = new Date(Date.now() - (parseInt(days) || 1) * 24 * 60 * 60 * 1000).toISOString();
  let query = `SELECT character_name, message FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (server && server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }
  if (category && category !== 'all') { params.push(category); query += ` AND category = $${params.length}`; }
  
  try {
    const result = await pool.query(query, params);
    const nicknames = new Set(result.rows.map(r => r.character_name));
    const freq = {};
    
    // 다단어 묶기
    const MULTI_WORDS = {
      '심판의 칼날': '심판의칼날', '파멸의 로브': '파멸의로브',
      '나이트 브링어': '나이트브링어', '태양과 달의 검': '태달검'
    };

    result.rows.forEach(r => {
      let msg = r.message;

      for (const [key, val] of Object.entries(MULTI_WORDS)) {
        msg = msg.replace(new RegExp(key, 'g'), val);
      }

      // 💣 1차 핵폭격: 거래/파티 단어 싹쓸이
      msg = msg.replace(/구매합니다|판매합니다|구합니다|팝니다|삽니다|팔아요|사요|구함|팜|삼|구매|판매|구입|개당|제시|흥정|선받|풀팟|가실분/g, ' ');
      
      // 💣 2차 핵폭격: 숫자 + 단위 (작가님이 말씀하신 '뭉', '뭉치' 완벽 추가!)
      msg = msg.replace(/[0-9]+(명|인|채|릴|팟|숲|억|만|골|골드|수표|랩|렙|옵|관|뭉|뭉치)/g, ' ');
      
      // 💣 3차 핵폭격: 채널 
      msg = msg.replace(/(채널|ch)\s*[0-9]+|[0-9]+\s*(채널|ch|채)/gi, ' ');

      // 먼지 털어낸 후 단어 쪼개기
      const words = msg.split(/[\s\[\]\(\)#:,.!?~ㅋㅎ/\\]+/).filter(w => w.length >= 2);
      
      words.forEach(w => {
        if (nicknames.has(w)) return; // 닉네임 컷
        if (/^[0-9]+$/.test(w)) return; // 순수 숫자 컷
        
        // 🗡️ 최종 확인사살 (정규식을 뚫고 들어온 잔당 처리)
        if (w.endsWith('요')) return; // '~요' 로 끝나는 단어 (가요, 해요 등) 사살
        if (w.includes('삽니다') || w.includes('팝니다') || w.includes('구매') || w.includes('판매') || w.includes('구합')) return;
        if (w.includes('채널')) return; // '채널' 글자가 들어간 모든 단어 사살
        
        // 🔥 4차 핵폭격: 구구, 있음, 50있 등 거슬리는 단어 완벽 차단!
        if (w.includes('구구') || w.includes('있음') || /[0-9]+있/.test(w) || /^[0-9]+-$/.test(w)) return;
        
        freq[w] = (freq[w] || 0) + 1;
      });
    });
    res.json({ keywords: Object.entries(freq).sort((a,b) => b[1]-a[1]).slice(0, 20).map(([word, count]) => ({ word, count })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── 🔥 복구된 파티 모집 현황 API (교역 추가, 시간 데이터 포함) ──
// ── 🔥 복구된 파티 모집 현황 API (길드 싹 빼기 & 중복 파티장 제거 완료) ──
app.get('/api/stats/party', async (req, res) => {
  const { server, days } = req.query;
  const since = new Date(Date.now() - (parseInt(days) || 1) * 24 * 60 * 60 * 1000).toISOString();
  
  // 🔥 중복 제거를 위해 character_name(닉네임)을 추가로 불러옵니다!
  let query = `SELECT character_name, message, date_send FROM horn WHERE category = 'party' AND message !~ '길드|길원|길모|성장' AND date_send >= $1`;
  const params = [since];
  
  if (server && server !== 'all') { 
    params.push(server); 
    query += ` AND server_name = $${params.length}`; 
  }
  query += ` ORDER BY date_send DESC`; 
  
  try {
    const result = await pool.query(query, params);
    
    const dungeons = {
      '브리레흐 (1~3관)': { count: 0, recent: [], color: 'blue' },
      '브리레흐 (4관)': { count: 0, recent: [], color: 'bri4' },
      '크롬바스': { count: 0, recent: [], color: 'red' },
      '글렌베르나': { count: 0, recent: [], color: 'teal' },
      '몽환의 라비': { count: 0, recent: [], color: 'purple' },
      '교역': { count: 0, recent: [], color: 'gold' },
      '기타': { count: 0, recent: [], color: 'etc' }
    };
    
    const seenParty = new Set(); // 🔥 중복 파티 방지용 세트 (기억 장치)

    result.rows.forEach(r => {
      let key = '기타';
      const msg = r.message;
      
      if (/4관/.test(msg)) key = '브리레흐 (4관)';
      else if (/브리|브레|1[~-]3관?/.test(msg)) key = '브리레흐 (1~3관)';
      else if (/크롬|크일|크쉬|빠스/.test(msg)) key = '크롬바스';
      else if (/글렌|글매|글렴|글쉬/.test(msg)) key = '글렌베르나';
      else if (/몽라|몽몽라|몽환/.test(msg)) key = '몽환의 라비';
      else if (/필발|왕복|필리아|코르|발레스|켈발|항교|교역/.test(msg)) key = '교역';
      
      // 🔥 핵심 로직: "닉네임 + 던전명" 조합이 이미 기억 장치에 있다면 카운트 무시!
      const dupKey = `${r.character_name}_${key}`;
      if (seenParty.has(dupKey)) return; 
      seenParty.add(dupKey); // 처음 보는 파티장이면 기억 장치에 등록

      dungeons[key].count++;
      if (dungeons[key].recent.length < 5) {
        dungeons[key].recent.push({ message: msg, date: r.date_send });
      }
    });
    
    // 🔥 뻥튀기 거뿔이 제거된 '진짜 순수 파티 수'를 다시 계산
    const realTotal = Object.values(dungeons).reduce((sum, d) => sum + d.count, 0);
    
    res.json({ total: realTotal, dungeons: Object.entries(dungeons).map(([name, info]) => ({ name, ...info })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── (여기서부터 복사) ───

// ── 🌤️ AI 에린 기상캐스터 API ──
let aiWeatherCache = { time: 0, data: {} };

app.get('/api/stats/lobby-weather', async (req, res) => {
  const server = req.query.server || 'all';
  const now = Date.now();
  
  // 🔥 AI API 요금 폭탄 방지용 캐싱 (10분에 한 번만 갱신)
  if (now - aiWeatherCache.time < 10 * 60 * 1000 && aiWeatherCache.data[server]) {
    return res.json({ weather: aiWeatherCache.data[server] });
  }
  
  if (!genAI) return res.json({ weather: "현재 에린은 평화롭습니다." });

  try {
    let query = `SELECT message FROM horn ORDER BY date_send DESC LIMIT 60`;
    const params = [];
    if (server !== 'all') {
      query = `SELECT message FROM horn WHERE server_name = $1 ORDER BY date_send DESC LIMIT 60`;
      params.push(server);
    }
    const result = await pool.query(query, params);
    const messages = result.rows.map(r => r.message).join('\n');

    const prompt = `너는 마비노기 기상캐스터야. 다음 최근 거뿔 60개를 보고 현재 서버 분위기를 재밌고 센스있게 딱 '한 줄'로 요약해. (예시: 현재 류트 서버는 '나이트 브링어' 매물 눈치싸움과 '브리레흐 4관' 구인으로 매우 뜨겁습니다!)
    [최근 거뿔]
    ${messages}`;

    const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' });
    const aiResult = await model.generateContent(prompt);
    let weatherText = aiResult.response.text().trim().replace(/^["']|["']$/g, '');

    if(!aiWeatherCache.data) aiWeatherCache.data = {};
    aiWeatherCache.data[server] = weatherText;
    aiWeatherCache.time = now;

    res.json({ weather: weatherText });
  } catch(e) {
    res.json({ weather: "마나 흐름이 불안정하여 날씨를 파악할 수 없습니다." });
  }
});

// ── 🏴‍☠️ 거불/특수 거래 (어둠의 거래소) API ──
app.get('/api/stats/blackmarket', async (req, res) => {
  const server = req.query.server || 'all';
  // 데이터가 적을 수 있으니 최근 3일치 스캔
  const since = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(); 

  let query = `SELECT message, date_send FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }

  try {
    const result = await pool.query(query, params);
    
    // 🔥 작가님 기획 아이템 5대장
    const trends = {
      '브리 구슬': { prices: [] },
      '인능상': { prices: [] },
      '거불 인보포': { prices: [] },
      '거불 시암': { prices: [] },
      '수세공': { prices: [] }
    };

    result.rows.forEach(r => {
      // 띄어쓰기 다 무시하고 검색하기 위해 공백 제거
      const msg = r.message.replace(/\s+/g, ''); 

      // 1. 브리 구슬 (뀨, 구구, 구슬구매, 거불구슬) + 뒤에 숫자(가격)
      const beadMatch = msg.match(/(뀨|구구|구슬구매|거불구슬)(\d+)(숲|만)?/);
      if (beadMatch) trends['브리 구슬'].prices.push({ time: r.date_send, price: parseInt(beadMatch[2]) });

      // 2. 인능상
      const inMatch = msg.match(/(인능상|인챈트능력의상승스크롤)(\d+)(숲|만)?/);
      if (inMatch) trends['인능상'].prices.push({ time: r.date_send, price: parseInt(inMatch[2]) });

      // 3. 거불 인보포
      const inboMatch = msg.match(/(거불인보포|인보포거불)(\d+)(숲|만)?/);
      if (inboMatch) trends['거불 인보포'].prices.push({ time: r.date_send, price: parseInt(inboMatch[2]) });

      // 4. 거불 시암
      const siamMatch = msg.match(/(거불시암|시암거불)(\d+)(숲|만)?/);
      if (siamMatch) trends['거불 시암'].prices.push({ time: r.date_send, price: parseInt(siamMatch[2]) });

      // 5. 수세공 (수세공, 거불수세공)
      const suseMatch = msg.match(/(거불수세공|수세공)(\d+)(숲|만)?/);
      if (suseMatch) trends['수세공'].prices.push({ time: r.date_send, price: parseInt(suseMatch[2]) });
    });

    const summary = {};
    for (const [item, data] of Object.entries(trends)) {
      if (data.prices.length > 0) {
        // 비정상적으로 높거나 낮은 가격(묶음 판매 등) 대충 걸러내기 (10000 초과 컷)
        const validPrices = data.prices.filter(p => p.price > 0 && p.price < 10000);
        if (validPrices.length > 0) {
          // 시간순 정렬
          validPrices.sort((a,b) => new Date(a.time) - new Date(b.time));
          // 최근 평균가 계산 (마지막 5개 기준)
          const recent = validPrices.slice(-5);
          const avg = Math.round(recent.reduce((a, b) => a + b.price, 0) / recent.length);
          
          summary[item] = { avg, count: validPrices.length, history: validPrices };
        }
      }
    }
    res.json(summary);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── 💰 거뿔 시세 발골기 (Market API) ──
app.get('/api/stats/market', async (req, res) => {
  const { server, days } = req.query;
  const since = new Date(Date.now() - (parseInt(days) || 1) * 24 * 60 * 60 * 1000).toISOString();
  
  // 🔥 기본 쿼리문
  let query = `SELECT server_name, character_name, message, date_send FROM horn WHERE category = 'trade' AND date_send >= $1`;
  const params = [since];
  
  // 🔥 서버 조건이 있으면 먼저 붙여주기
  if (server && server !== 'all') { 
    params.push(server); 
    query += ` AND server_name = $${params.length}`; 
  }
  
  // 🔥 정렬과 리밋은 무조건 맨 마지막에!
  query += ` ORDER BY date_send DESC LIMIT 500`;
  
  try {
    const result = await pool.query(query, params);
    const trades = [];
    
    // 마비노기식 거래 정규표현식: [아이템] + [가격 숫자.숫자] + [억/숲/만/골드] + [팜/삼]
    const tradeRegex = /(.*?)\s+([0-9]+(?:\.[0-9]+)?)\s*(억|숲|만|골드|수표)\s*(팝니다|삽니다|판매|구매|팜|삼|팔아요|사요)/;

    result.rows.forEach(r => {
      const match = r.message.match(tradeRegex);
      if (match) {
        let item = match[1].replace(/^[\[\(<【].*?[\]\)>】]/, '').replace(/[~!@#$^&*]/g, '').trim();
        const price = parseFloat(match[2]);
        const unit = match[3];
        const action = match[4].includes('사') || match[4].includes('구매') || match[4].includes('삼') ? '구매' : '판매';
        
        if (item.length > 1) {
          trades.push({
            server: r.server_name, character: r.character_name,
            item: item, price: price, unit: unit, action: action,
            time: r.date_send, original: r.message
          });
        }
      }
    });
    
    res.json({ total_scanned: result.rows.length, extracted: trades.length, trades });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// 닉네임 검색 (아까 날아간 기본 검색 기능 복구)
app.get('/api/user/:name', async (req, res) => {
  const name = req.params.name;
  try {
    const result = await pool.query(`
      SELECT server_name, message, date_send, category
      FROM horn
      WHERE character_name = $1
      ORDER BY date_send DESC
      LIMIT 200
    `, [name]);

    const rows = result.rows;
    if (rows.length === 0) return res.json({ found: false, name });

    const total = rows.length;
    const servers = {};
    const categories = { party: 0, trade: 0, guild: 0, etc: 0 };
    const hourMap = new Array(24).fill(0);

    rows.forEach(r => {
      servers[r.server_name] = (servers[r.server_name] || 0) + 1;
      categories[r.category] = (categories[r.category] || 0) + 1;
      const h = (new Date(r.date_send).getUTCHours() + 9) % 24;
      hourMap[h]++;
    });

    const peakHour = hourMap.indexOf(Math.max(...hourMap));
    const recentMessages = rows.slice(0, 10).map(r => r.message);

    res.json({
      found: true, name, total, servers, categories, hourMap, peakHour, recentMessages,
      oldest: rows[rows.length - 1].date_send, newest: rows[0].date_send,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// AI 분석 (메타데이터 + 고인물 프롬프트 적용된 매운맛 버전)
app.get('/api/user/:name/analyze', async (req, res) => {
  const name = req.params.name;
  if (!genAI) return res.status(500).json({ error: 'Gemini API 키가 없어요' });

  try {
    const result = await pool.query(`
      SELECT message, category, date_send, server_name 
      FROM horn
      WHERE character_name = $1 
      ORDER BY date_send DESC 
      LIMIT 100
    `, [name]);

    const rows = result.rows;
    if (rows.length === 0) return res.json({ found: false });

    const total = rows.length;
    let partyCount = 0, tradeCount = 0, guildCount = 0, etcCount = 0;
    const hourMap = new Array(24).fill(0);

    const messages = rows.map(r => {
      if (r.category === 'party') partyCount++;
      else if (r.category === 'trade') tradeCount++;
      else if (r.category === 'guild') guildCount++;
      else etcCount++;

      const h = (new Date(r.date_send).getUTCHours() + 9) % 24;
      hourMap[h]++;
      return `[${h}시] ${r.message}`;
    }).join('\n');

    const peakHour = hourMap.indexOf(Math.max(...hourMap));
    const mainCategory = Object.entries({ 파티: partyCount, 거래: tradeCount, 길드홍보: guildCount, 일반잡담: etcCount })
      .sort((a, b) => b[1] - a[1])[0][0];

    const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' });

    const prompt = `
너는 마비노기 20년 차 초고인물 밀레시안이야.
다음은 유저 "${name}"의 최근 거뿔(전체 채팅) 기록과 통계 데이터야.

[유저 통계 힌트]
- 가장 거뿔을 많이 부는 시간: ${peakHour}시
- 가장 많이 쓰는 거뿔 목적: ${mainCategory} (파티:${partyCount}, 거래:${tradeCount}, 길드:${guildCount}, 일반:${etcCount})

[최근 거뿔 내용]
${messages}

이 데이터를 바탕으로 이 유저의 에린 플레이 스타일을 아주 날카롭고 재밌게 프로파일링 해줘.

🚨 [절대 지켜야 할 마비노기 분석 룰] 🚨
1. 고인물의 '뉴비 호소' 기만: 마비노기 고인물들은 본인을 '뉴비'라고 기만하는 문화가 있어. 거뿔에 "뉴비 사냥가고 싶어요"라고 적혀 있어도, 브리 레흐, 크롬바스, 글렌베르나 같은 최상위 던전 파티를 구하고 있다면 그건 100% 썩은물(고인물)의 앙탈이야. 절대 속아서 '초보', '테흐 두인 입문자'라고 분석하지 마!
2. 던전 이름 창조 금지: '브리'는 무조건 '브리 레흐' 던전을 뜻해. '브리흐네', '브리흐네 릴레이 미션' 같은 이상한 던전 이름을 절대 창조하지 마.
3. 마비노기 용어 및 은어 사전:
   - 뀨 = 구구 = 구슬구매 (파티 참여 조건으로 자기가 입장 재화(구슬)를 돈 주고 사겠다는 뜻. 재력가 전투광임)
   - 1릴, 2릴 = 1번 반복, 2번 반복 사냥
   - 세바 = 세인트 바드, 세가 = 세이크리드 가드, 엘나 = 엘레멘탈 나이트, 닼메 = 다크 메이지, 퓨파/뜌따 = 퓨리 파이터
   - 숲 = 인게임 수표 (고액 화폐)

[마비노기 유저 유형 가이드라인]
- 전투광 / 결사대장: 브리 레흐 등 최상위 던전을 돌며 '뀨(구슬)' 구매까지 불사하는 진성 전투광. (특징: 뉴비 코스프레)
- 다클라 쌀먹러: 폐지 줍기, 숲(골드) 판매
- 에린의 거상: 혐사, 경매장 수수료 아끼려고 거뿔로 비싼 템 거래
- 확성기 빌런(뻘뿔러): 새벽에 쓸데없는 잡담 낭비

아래 JSON 형식으로만 딱 떨어지게 답변해. 다른 말은 절대 추가하지 마.
{
  "type": "유저 칭호 (예: 뉴비 코스프레하는 브리 레흐 공장장, 구슬 물주 뜌따, 던바튼 1채널 거상 등 재미있게)",
  "description": "이 유저는 어떤 스타일로 게임을 즐기는지 마비노기 세계관에 맞게 2~3문장으로 재미있게 요약",
  "traits": ["핵심 특징 1 (예: 지독한 기만자)", "핵심 특징 2 (예: 뀨(구슬) 구매 불사함)", "핵심 특징 3"],
  "activeTime": "주로 언제 접속하는지 (예: 저녁 피크타임)",
  "mainActivity": "이 유저가 마비노기에서 주로 하는 짓"
}`;

    const result2 = await model.generateContent(prompt);
    const text = result2.response.text().replace(/```json|```/g, '').trim();
    const analysis = JSON.parse(text);

    res.json({ found: true, name, total: rows.length, analysis });
  } catch (e) {
    console.error('[AI 분석 오류]', e.message);
    res.status(500).json({ error: e.message });
  }
});

//

async function start() {
  await initDB();
  app.listen(PORT, () => { console.log(`\n🎺 백엔드 서버 시작 (포트: ${PORT})`); fetchAll(); });
  cron.schedule('0 0 * * *', generateDailySummary, { timezone: "Asia/Seoul" });
  
  // 🔥 429 방지용 60초 주기 (매우 중요)
  setInterval(() => { fetchAll().catch(console.error); }, 900000); // 100초 = 하루 약 864회 × 4서버 
}
start().catch(console.error);