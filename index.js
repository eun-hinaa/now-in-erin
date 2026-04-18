require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const { Pool } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const app = express();

// 1. 기본 보안 헬멧 쓰기 (해커들이 훔쳐보기 어렵게 만듦)
app.use(helmet({
  contentSecurityPolicy: false
}));

// 🚨 [추가된 코드] Railway 같은 프록시 환경에서 진짜 IP를 인식하도록 설정!
app.set('trust proxy', 1);

// 2. CORS 엄격하게 설정 (내 깃허브 사이트에서만 서버에 말 걸 수 있게 철벽 방어!)
app.use(cors({
  origin: ['https://eun-hinaa.github.io', 'http://localhost:3000'],
  methods: ['GET']
}));

app.use(express.json());

// 3. Rate Limit (나쁜 놈들이 서버 터뜨리려고 F5 연타하는 걸 막아줌)
const apiLimiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1분 동안
  max: 100,                // 100번까지만 접속 허용! 그 이상은 차단.
  message: { error: '마나의 흐름이 불안정합니다. 잠시 후 다시 시도해주세요.' } // 에러 메시지
});

// 주소가 /api/ 로 시작하는 모든 요청에 위의 방어막(apiLimiter)을 씌움
app.use('/api/', apiLimiter);

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
    
    -- 🔥 누락되었던 AI 분석 저장소 자동 생성! 🔥
    CREATE TABLE IF NOT EXISTS user_analysis (
      character_name TEXT PRIMARY KEY,
      keywords TEXT[], 
      analysis_json JSONB, 
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

function classify(msg) {
  if (/길드|길원|길모|성장/.test(msg)) return 'guild';
  // 🔥 거래를 먼저 체크하게 위로 올리고, 은어(팜, 삼, 사요 등) 완벽 추가!
  if (/팝니다|팝|팜|판매|삽니다|삼|구매|구입|팔아요|사요|얼마|골드|가격|숲|제시|흥정/.test(msg)) return 'trade';
  // 그 다음 파티 체크
  if (/파티|구함|모집|인원|\/\d|[0-9]\/[0-9]/.test(msg)) return 'party';
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
    // 🔥 넥슨 API 고장 내던 파라미터 제거! 가장 잘 돌던 순정 코드로 원상복구
    const url = `https://open.api.nexon.com/mabinogi/v1/horn-bugle-world/history?server_name=${encodeURIComponent(serverName)}`;
    const res = await fetch(url, { headers: { 'x-nxopen-api-key': API_KEY } });

    if (!res.ok) return 0;

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
    if (newCount > 0) console.log(`[${serverName}] ${newCount}건 신규 저장 완료!`);
    return newCount;
  } catch (e) {
    return 0;
  }
}

async function fetchAll() {
  console.log('📡 전 서버 데이터 순차 발굴 시작... (메모리 절약 모드)');
  
  for (const server of SERVERS) {
    await fetchServer(server);
    // 💡 다음 서버 거뿔을 긁어오기 전에 3초 동안 숨 고르기 (메모리 쉴 시간 주기)
    await new Promise(resolve => setTimeout(resolve, 3000));
  }
  console.log('✅ 전 서버 발굴 완료!');
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
  const server = req.query.server || 'all';
  // 최근 24시간 데이터만 가져옵니다
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(); 
  
  let query = `SELECT server_name, character_name, message, date_send FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }

  try {
    const result = await pool.query(query, params);
    
    // 1. 서버별 뿔피리 화력
    const serverCounts = { '류트': 0, '만돌린': 0, '하프': 0, '울프': 0 };
    // 2. 핫 채널 (숫자+채널)
    const channelCounts = {};
    // 3. 에린 감정 지수 (ㅋㅋ vs ㅠㅠ)
    let laughCount = 0;
    let cryCount = 0;
    // 4. 단어 빈도수 (간단한 키워드 추출)
    const wordCounts = {};

    result.rows.forEach(r => {
      const msg = r.message;
      
      // 서버 카운트
      if (serverCounts[r.server_name] !== undefined) serverCounts[r.server_name]++;

      // 감정 지수 체크
      if (/(ㅋㅋ+)/.test(msg)) laughCount++;
      if (/(ㅠㅠ+|ㅜㅜ+)/.test(msg)) cryCount++;

      // 핫 채널 체크 (예: 4채, 14채널, 20ch)
      const chMatch = msg.match(/([0-9]{1,2})\s*(채널|채|ch)/i);
      if (chMatch) {
        const chNum = chMatch[1] + '채널';
        channelCounts[chNum] = (channelCounts[chNum] || 0) + 1;
      }

      // 핫 키워드 추출 (불용어 제외, 2글자 이상)
      const words = msg.replace(/[^\w가-힣]/g, ' ').split(/\s+/);
      const stopWords = ['팝니다', '삽니다', '판매', '구매', '거뿔', '있는', '구함', '팔아요', '사요'];
      words.forEach(w => {
        if (w.length >= 2 && !stopWords.includes(w) && !/^[0-9]+$/.test(w)) {
          wordCounts[w] = (wordCounts[w] || 0) + 1;
        }
      });
    });

    // 가장 많이 언급된 채널 찾기
    const topChannel = Object.keys(channelCounts).sort((a, b) => channelCounts[b] - channelCounts[a])[0] || '측정 불가';
    
    // 가장 많이 언급된 키워드 찾기 (상위 1개)
    const topKeyword = Object.keys(wordCounts).sort((a, b) => wordCounts[b] - wordCounts[a])[0] || '측정 불가';

    // 종합 데이터 응답
    res.json({
      total: result.rows.length,
      serverCounts,
      topChannel,
      emotion: { laugh: laughCount, cry: cryCount },
      topKeyword
    });

  } catch (e) { 
    res.status(500).json({ error: e.message }); 
  }
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
    
    // 🔥 KST 기준 오늘 자정(00:00:00) 문자열을 JS에서 강제 생성! DB 에러 원천 차단!
    const nowKST = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
    const todayStr = `${nowKST.getFullYear()}-${String(nowKST.getMonth() + 1).padStart(2, '0')}-${String(nowKST.getDate()).padStart(2, '0')} 00:00:00+09`;

    await Promise.all(SERVERS.map(async (server) => {
      const result = await pool.query(`
        SELECT character_name, COUNT(*) as count 
        FROM horn 
        WHERE server_name = $1 AND date_send >= $2
        GROUP BY character_name 
        ORDER BY count DESC 
        LIMIT 1
      `, [server, todayStr]);
      
      if (result.rows.length > 0) {
        const name = result.rows[0].character_name;
        kings[server] = { 
          masked: name.length <= 2 ? name[0] + 'X' : name.slice(0, 2) + 'X'.repeat(name.length - 2), 
          count: parseInt(result.rows[0].count) 
        };
      } else {
        kings[server] = { masked: '조용한 에린', count: 0 };
      }
    }));
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
      
      // 🔥 1. 마비노기 시스템이 강제로 붙인 '&<' '&>' 껍데기를 예쁜 대괄호로 정화!
      let cleanMsg = r.message.replace(/&<(.*?)&>/, '[$1]');
      
      // 🔥 2. 정화된 cleanMsg를 기준으로 길드명을 추출합니다. (r.message -> cleanMsg)
      const bracketMatch = cleanMsg.match(/[\[\(【<「『](.*?)[\]\)】>」』]/);
      
      if (bracketMatch) {
        guildName = bracketMatch[1].replace(/채널\d+/, '').trim();
      } else {
        const textMatch = cleanMsg.match(/([가-힣a-zA-Z0-9]{2,10})(?:에서| 길드)/);
        if (textMatch) {
          const word = textMatch[1];
          if (!/[실는할운고]$/.test(word) && !/초보|성인|매너|친목|신생|환영|모집|가입|성장|같이/.test(word)) {
            guildName = word;
          }
        }
      }

      const chMatch = cleanMsg.match(/([0-9]+)\s*(?:채|ch)/i);
      const channel = chMatch ? chMatch[1] + '채널' : '채널 미상';

      if (guildName && guildName.length >= 2 && !/^\d+$/.test(guildName)) {
        if (!guildMap[guildName]) guildMap[guildName] = { name: guildName, messages: [], server: r.server_name };
        if (guildMap[guildName].messages.length < 3) {
          // 🔥 3. 화면에 뿌려줄 때도 정화된 cleanMsg를 보냅니다!
          guildMap[guildName].messages.push({ text: cleanMsg, character: r.character_name, channel: channel, date: r.date_send });
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
      
      const MULTI_WORDS = {
        '심판의 칼날': '심판의칼날', '파멸의 로브': '파멸의로브',
        '나이트 브링어': '나이트브링어', '태양과 달의 검': '태달검'
      };

      // 🔥 추가: 도배 방지용 기억 장치 (키워드 탭에서만 작동함!)
      const uniqueMessages = new Set();

      result.rows.forEach(r => {
        // 🔥 추가: 닉네임과 내용이 완전히 똑같으면 키워드 집계에서는 쿨하게 패스
        const dupKey = `${r.character_name}_${r.message}`;
        if (uniqueMessages.has(dupKey)) return;
        uniqueMessages.add(dupKey);

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
    // 🚨 핵심: 수집된 거뿔이 없으면 AI 부르지 말고 여기서 컷!
    if (result.rows.length === 0) {
      return res.json({ weather: "아직 수집된 거뿔이 없어 에린이 아주 조용합니다." });
    }

    const messages = result.rows.map(r => r.message).join('\n');

    const prompt = `너는 마비노기 서버 분위기를 객관적이고 재밌게 알려주는 안내자야.
다음 최근 거뿔 60개를 보고 현재 서버 분위기를 센스있게 딱 '한 줄'로 요약해.

🚨 [절대 지켜야 할 규칙] 🚨
1. 본인을 특정 NPC(나오 등)나 기상캐스터라고 칭하지 마. (예: "안녕하세요 나오입니다" 절대 금지)
2. 거뿔에 언급된 특정 유저의 닉네임은 절대로 포함하지 마.
3. 던전 이름은 반드시 공식 명칭인 '브리 레흐'로만 출력해. 이상한 변형은 금지야.
4. 결과는 부가 설명 없이 딱 요약된 한 줄 텍스트만 출력해.

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
  const since = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString(); // 고정 6시간

  let query = `SELECT server_name, character_name, message, date_send FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }

  try {
    const result = await pool.query(query, params);
    const trends = {
      '브리 구슬': { prices: [] },
      '붕마정': { prices: [] },
      '크롬 장기': { prices: [] },
      '인능상': { prices: [] },
      '거불 인보포': { prices: [] },
      '수세공': { prices: [] },
      '인형가방 (떨굼)': { prices: [] }
    };

    // 🔥 작가님 전용 기적의 수학법 (1.0 = 1억 = 10000숲 / 1550 = 1550숲)
    const parseSoop = (numStr, unit) => {
      let num = parseFloat(numStr);
      if (unit === '억') return num * 10000;
      if (unit === '천만' || unit === '천') return num * 1000; 
      if (unit === '만' || unit === '숲') return num; 
      
      // 단위 생략 시 추론: 10 미만은 '억'으로, 그 이상은 '숲'으로 간주
      if (num < 10) return num * 10000; 
      return num; 
    };

    result.rows.forEach(r => {
      const msg = r.message.replace(/\s+/g, ''); 
      const itemData = { 
        time: r.date_send, 
        character: r.character_name, 
        server: r.server_name, 
        message: r.message 
      };

      const extractMatch = (regex, itemName) => {
        const match = msg.match(regex);
        if (match) {
          let price = parseSoop(match[2], match[3]);

          // 🔥 수세공 묶음 판매(100개, 400개 등) 개당 단가 보정 로직
          if (itemName === '수세공' && price > 200) {
            const commonQtys = [400, 100, 50, 10];
            for (let q of commonQtys) {
              let unitPrice = price / q;
              if (unitPrice >= 30 && unitPrice <= 70) { 
                price = unitPrice; 
                break; 
              }
            }
          }
          trends[itemName].prices.push({ ...itemData, price: Math.round(price) });
        }
      };

      extractMatch(/(뀨|구구|구슬구매|거불구슬)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '브리 구슬');
      extractMatch(/(붕마정|붕괴된마력의정수)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '붕마정');
      extractMatch(/(아다만|아다만티움|글기깃|글기심|장기)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '크롬 장기');
      extractMatch(/(인능상|인챈트능력의상승스크롤)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '인능상');
      extractMatch(/(거불인보포|인보포거불)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '거불 인보포');
      extractMatch(/(거불수세공|수세공)([0-9]*\.?[0-9]+)(억|천만?|숲|만)?/, '수세공');

      if (msg.includes('가방') && /떨굼|떨식|깐/.test(msg)) {
        const bagM = msg.match(/([0-9]*\.?[0-9]+)(억|천만?|숲|만)?(에|으로)?(팝니다|삽니다|판매|구매|팜|삼|팔아요|사요|구함|구해)/);
        if (bagM) trends['인형가방 (떨굼)'].prices.push({ ...itemData, price: Math.round(parseSoop(bagM[1], bagM[2])) });
      }
    });

    const summary = {};
    for (const [item, data] of Object.entries(trends)) {
      if (data.prices.length > 0) {
        // 🔥 통계적 이상치 제거 (IQR) - 묶음 판매/혐사 자동 필터
        const sorted = [...data.prices].sort((a, b) => a.price - b.price);
        const q1 = sorted[Math.floor(sorted.length * 0.25)].price;
        const q3 = sorted[Math.floor(sorted.length * 0.75)].price;
        let iqr = q3 - q1 || q3 * 0.5;
        
        let validPrices = data.prices.filter(p => p.price >= q1 - 1.5 * iqr && p.price <= q3 + 1.5 * iqr);
        if (validPrices.length === 0) validPrices = data.prices;

        validPrices.sort((a,b) => new Date(a.time) - new Date(b.time));
        
        // 🔥 서버별 평균 계산 (정상 데이터 기반)
        const serverStats = {};
        validPrices.forEach(p => {
          if (!serverStats[p.server]) serverStats[p.server] = { sum: 0, count: 0 };
          serverStats[p.server].sum += p.price;
          serverStats[p.server].count++;
        });
        
        const serverAvgs = {};
        for (const [srv, stat] of Object.entries(serverStats)) {
          serverAvgs[srv] = Math.round(stat.sum / stat.count);
        }

        const recent = validPrices.slice(-10);
        const avg = Math.round(recent.reduce((a, b) => a + b.price, 0) / recent.length);
        
        summary[item] = { avg, count: validPrices.length, serverAvgs, history: validPrices.reverse() };
      }
    }
    res.json(summary);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── 💰 일반 거뿔 거래 시세 (Market API) ──
app.get('/api/stats/market', async (req, res) => {
  const { server, days } = req.query;
  const since = new Date(Date.now() - (parseInt(days) || 1) * 24 * 60 * 60 * 1000).toISOString();
  
  let query = `SELECT server_name, character_name, message, date_send FROM horn WHERE (category = 'trade' OR message ~ '팝니다|삽니다|판매|구매|팜|삼|팔아요|사요') AND date_send >= $1`;
  const params = [since];
  
  if (server && server !== 'all') { 
    params.push(server); 
    query += ` AND server_name = $${params.length}`; 
  }
  
  query += ` ORDER BY date_send DESC LIMIT 500`;
  
  try {
    const result = await pool.query(query, params);
    const trades = [];
    
    // 🚨 기존의 깐깐했던 tradeRegex 코드를 지우고 아래 무적 코드로 덮어씌우세요!
    const tradeRegex = /(.*?)(?:[\s:]+)([0-9]{1,5}(?:\.[0-9]{1,2})?)\s*(억|숲|만|골드|수표)?(?:에|으로|에만|씩)?\s*(팝니다|삽니다|판매|구매|팜|삼|팔아요|사요)/;

    result.rows.forEach(r => {
      const match = r.message.match(tradeRegex);
      if (match) {
        let item = match[1].replace(/^[\[\(<【].*?[\]\)>】]/, '').replace(/[~!@#$^&*]/g, '').trim();
        const rawNum = match[2];
        const unit = match[3]; // '억'이나 '숲'을 안 썼으면 undefined가 됨
        const action = match[4].includes('사') || match[4].includes('구매') || match[4].includes('삼') ? '구매' : '판매';
        
        // 🔥 기적의 마비노기 수학법 (단위가 없어도 10 미만이면 '억', 이상이면 '숲'으로 찰떡같이 계산)
        const priceSoop = (numStr, unitStr) => {
          let num = parseFloat(numStr);
          if (unitStr === '억') return num * 10000;
          if (unitStr === '만' || unitStr === '숲') return num;
          if (num < 10) return num * 10000; 
          return num; 
        };

        if (item.length > 1) {
          trades.push({
            server: r.server_name, character: r.character_name,
            item: item, price: priceSoop(rawNum, unit), unit: '숲', 
            action: action, time: r.date_send, original: r.message
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

// 1. AI 분석 API 수술 (방탄 처리 + DB 저장)
app.get('/api/user/:name/analyze', async (req, res) => {
  const name = req.params.name;
  if (!genAI) return res.status(500).json({ error: 'Gemini API 키가 없어요' });

  try {
    const result = await pool.query(`
      SELECT message, category, date_send, server_name FROM horn
      WHERE character_name = $1 ORDER BY date_send DESC LIMIT 100
    `, [name]);

    const rows = result.rows;
    if (rows.length === 0) return res.json({ found: false });

    let partyCount = 0, tradeCount = 0, guildCount = 0, etcCount = 0;
    const messages = rows.map(r => {
      if (r.category === 'party') partyCount++;
      else if (r.category === 'trade') tradeCount++;
      else if (r.category === 'guild') guildCount++;
      else etcCount++;
      return `[${new Date(r.date_send).getHours()}시] ${r.message}`;
    }).join('\n');

    // 🛡️ 핵심: 제미니에게 "무조건 JSON으로만 줘!" 라고 강제 설정
    const model = genAI.getGenerativeModel({ 
      model: 'gemini-2.5-flash',
      generationConfig: { responseMimeType: "application/json" } 
    });

    const prompt = `너는 마비노기 유저 프로파일러야. 아래 유저 "${name}"의 거뿔 데이터를 분석해서 반드시 JSON 형식으로만 답변해.

    [JSON 요구사항]
    1. "type": 유저의 성향을 나타내는 재미있는 칭호 (절대 '칭호'라는 단어 쓰지 말 것. 예: 낭만 가득한 요정)
    2. "description": 유저의 플레이 성향에 대한 유쾌한 분석 내용 2~3문장 (절대 '분석'이라는 단어 쓰지 말 것)
    3. "keywords": 유저 성향을 나타내는 밈(meme) 해시태그 4개 (단순 단어 추출 절대 금지. 반드시 #자본주의, #새벽반 처럼 성향을 유추해서 창작할 것)
    4. "activeTime": 주로 활동하는 시간대
    5. "mainActivity": 주로 하는 활동

    [데이터]\n${messages}`;

    const aiResponse = await model.generateContent(prompt);
    const rawText = aiResponse.response.text();
    
    // 정규식 없이 바로 파싱!
    const analysis = JSON.parse(rawText);

    // 💾 DB에 저장
    await pool.query(`
      INSERT INTO user_analysis (character_name, keywords, analysis_json, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (character_name) DO UPDATE SET keywords = EXCLUDED.keywords, analysis_json = EXCLUDED.analysis_json, updated_at = NOW()
    `, [name, analysis.keywords, analysis]);

    res.json({ found: true, name, analysis });
  } catch (e) {
    console.error(`[AI 분석 오류 - ${name}]`, e.message);
    res.status(500).json({ error: e.message });
  }
});


app.get('/api/user/:name/similar', async (req, res) => {
  const name = req.params.name;
  try {
    // 1. 현재 유저의 키워드 가져오기
    const userRes = await pool.query('SELECT keywords FROM user_analysis WHERE character_name = $1', [name]);
    if (userRes.rows.length === 0) return res.json({ similar: [] });

    const userKeywords = userRes.rows[0].keywords;

    // 2. 키워드가 겹치는 다른 유저 찾기 (PostgreSQL Array Overlap 사용)
    const similarRes = await pool.query(`
      SELECT character_name, keywords, (
        SELECT COUNT(*) FROM unnest(keywords) k WHERE k = ANY($1)
      ) as match_count
      FROM user_analysis
      WHERE character_name != $1
      ORDER BY match_count DESC
      LIMIT 5
    `, [userKeywords]);

    res.json({ similar: similarRes.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 🚨 [과금 방어 + 중복 완벽 차단] 릴레이 바통 터치 방식!
let isRunning = false;
let stopRequested = false;
let currentProcessedCount = 0;
let currentTargetLimit = 100;

app.get('/api/admin/start-safe', async (req, res) => {
  const limitQuery = parseInt(req.query.limit);
  if (limitQuery) currentTargetLimit = limitQuery;

  // ✨ 관리자 전용 실시간 모니터링 대시보드 (HTML + 자동 갱신 스크립트)
  const htmlResponse = `
    <!DOCTYPE html>
    <html lang="ko">
    <head>
      <meta charset="UTF-8">
      <title>AI 분석 공장 가동 현황</title>
      <style>
        body { font-family: 'Pretendard', -apple-system, sans-serif; background: #1e1e1e; color: #fff; padding: 40px; margin: 0; }
        .container { max-width: 800px; margin: 0 auto; }
        .box { background: #2d2d2d; padding: 30px; border-radius: 12px; margin-bottom: 20px; border-left: 6px solid #f4a261; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        h2 { color: #f4a261; margin-top: 0; font-size: 28px; }
        p { color: #aaa; font-size: 16px; }
        .stat { font-size: 22px; margin: 20px 0; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #444; padding-bottom: 10px; }
        .highlight { color: #2a9d8f; font-weight: 900; font-size: 32px; }
        .server-list { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin-top: 20px; }
        .server-box { background: #3d3d3d; padding: 20px; border-radius: 8px; font-size: 20px; display: flex; justify-content: space-between; align-items: center; }
        .server-name { color: #e9c46a; font-weight: bold; }
      </style>
      <script>
        // 3초마다 DB 통계 현황을 가져와서 화면에 채워 넣습니다!
        setInterval(async () => {
          try {
            const res = await fetch('/api/stats/progress');
            const data = await res.json();
            document.getElementById('total').innerText = data.total_horn_users.toLocaleString() + '명';
            document.getElementById('analyzed').innerText = data.analyzed_total.toLocaleString() + '명';
            document.getElementById('ryute').innerText = (data.server_users['류트'] || 0).toLocaleString() + '명';
            document.getElementById('mando').innerText = (data.server_users['만돌린'] || 0).toLocaleString() + '명';
            document.getElementById('harp').innerText = (data.server_users['하프'] || 0).toLocaleString() + '명';
            document.getElementById('wolf').innerText = (data.server_users['울프'] || 0).toLocaleString() + '명';
          } catch(e) {}
        }, 3000);
      </script>
    </head>
    <body>
      <div class="container">
        <div class="box">
          <h2>🚀 AI 프로파일링 공장 가동 중... (목표: ${currentTargetLimit}명)</h2>
          <p>창을 켜두시면 새로고침 없이 3초마다 아래 숫자가 실시간으로 올라갑니다!</p>
        </div>
        <div class="box" style="border-left-color: #2a9d8f;">
          <div class="stat"><span>📢 에린 전체 거뿔 유저 수</span> <span id="total" class="highlight">로딩중...</span></div>
          <div class="stat" style="border-bottom: none;"><span>✨ AI 분석 완료 유저 수</span> <span id="analyzed" class="highlight">로딩중...</span></div>
        </div>
        <div class="box" style="border-left-color: #e9c46a;">
          <h3 style="margin-top:0; font-size: 22px; color: #e9c46a;">서버별 거뿔 유저 발굴 현황</h3>
          <div class="server-list">
            <div class="server-box"><span class="server-name">류트</span> <span id="ryute" class="highlight" style="font-size:24px;">0명</span></div>
            <div class="server-box"><span class="server-name">만돌린</span> <span id="mando" class="highlight" style="font-size:24px;">0명</span></div>
            <div class="server-box"><span class="server-name">하프</span> <span id="harp" class="highlight" style="font-size:24px;">0명</span></div>
            <div class="server-box"><span class="server-name">울프</span> <span id="wolf" class="highlight" style="font-size:24px;">0명</span></div>
          </div>
        </div>
      </div>
    </body>
    </html>
  `;

  // 이미 돌고 있으면 대시보드 화면만 보여줌
  if (isRunning) {
    return res.send(htmlResponse);
  }
  
  isRunning = true;
  stopRequested = false;
  currentProcessedCount = 0;
  
  // 시작과 동시에 대시보드 화면 전송
  res.send(htmlResponse);

  // 🔥 백그라운드 무한 분석 루프
  (async () => {
    while (currentProcessedCount < currentTargetLimit && !stopRequested) {
      try {
        const target = await pool.query(`
          SELECT character_name 
          FROM horn 
          WHERE character_name NOT IN (SELECT character_name FROM user_analysis)
          GROUP BY character_name 
          ORDER BY COUNT(*) DESC 
          LIMIT 1
        `);

        if (target.rows.length === 0) {
          console.log('🎉 [종료] 에린의 모든 거뿔왕을 싹쓸이했습니다.');
          break;
        }

        const name = target.rows[0].character_name;
        const userMsgRes = await pool.query(`SELECT message FROM horn WHERE character_name = $1 ORDER BY date_send DESC LIMIT 50`, [name]);
        const messages = userMsgRes.rows.map(r => r.message).join('\n');

        const model = genAI.getGenerativeModel({ model: 'gemini-2.5-flash', generationConfig: { responseMimeType: "application/json" } });
        
        const prompt = `너는 마비노기 유저 프로파일러야. 아래 유저 "${name}"의 거뿔 데이터를 분석해서 반드시 JSON 형식으로만 답변해.

        [JSON 요구사항]
        1. "type": 유저의 성향을 나타내는 재미있는 칭호 (절대 '칭호'라는 단어 쓰지 말 것. 예: 낭만 가득한 요정)
        2. "description": 유저의 플레이 성향에 대한 유쾌한 분석 내용 2~3문장 (절대 '분석'이라는 단어 쓰지 말 것)
        3. "keywords": 유저 성향을 나타내는 밈(meme) 해시태그 4개 (단순 단어 추출 절대 금지. 반드시 #자본주의, #새벽반 처럼 성향을 유추해서 창작할 것)
        4. "activeTime": 주로 활동하는 시간대
        5. "mainActivity": 주로 하는 활동

[🚨절대 규칙🚨] 
        1. JSON의 key 값에 "칭호", "분석" 이라는 단어를 그대로 적지 마! 반드시 네가 창작한 내용을 적어.
        2. 부정적 단어(빌런, 비매너 등) 절대 금지. 유쾌하고 둥글게 포장할 것. 
        3. 본문 단어 단순 추출 금지. 대화 패턴으로 성향을 유추한 밈 해시태그(예: #새벽반, #자본주의)를 창작할 것.
        [데이터]\n${messages}`;

        const aiResponse = await model.generateContent(prompt);
        const analysis = JSON.parse(aiResponse.response.text());

        await pool.query(`
          INSERT INTO user_analysis (character_name, keywords, analysis_json, updated_at)
          VALUES ($1, $2, $3, NOW())
          ON CONFLICT (character_name) DO UPDATE 
          SET keywords = EXCLUDED.keywords, analysis_json = EXCLUDED.analysis_json, updated_at = NOW()
        `, [name, analysis.keywords, analysis]);
        
        currentProcessedCount++;
        console.log(`🛡️ [안전 모드: ${currentProcessedCount}/${currentTargetLimit}] ${name} 분석 완료!`);

        await new Promise(resolve => setTimeout(resolve, 2000));

      } catch (err) {
        console.error(`💥 [API 삑사리 - 2초 뒤 다음 사람 진행]`, err.message);
        await new Promise(resolve => setTimeout(resolve, 2000)); 
      }
    }
    
    console.log(`🎉 [안전 종료] 공장 가동이 끝났습니다. (총 ${currentProcessedCount}명 처리 완료)`);
    isRunning = false;
  })();
});

// 수동 비상정지 버튼
app.get('/api/admin/stop-safe', (req, res) => {
  if (isRunning) {
    stopRequested = true;
    res.send('<h2>🛑 현재 작업 중인 1명만 마저 끝내고 공장을 정지합니다.</h2>');
  } else {
    res.send('<h2>🤷‍♂️ 공장이 이미 멈춰 있습니다.</h2>');
  }
});

// 🚨 [관리자 전용] 불량 AI 데이터 싹 밀어버리기 (초기화)
app.get('/api/admin/reset-analysis', async (req, res) => {
  try {
    await pool.query('TRUNCATE TABLE user_analysis;');
    res.send('<h2>🧹 불량 분석 데이터 초기화 완료! DB가 깨끗해졌습니다.</h2>');
  } catch (err) {
    res.send(`에러 발생: ${err.message}`);
  }
});

async function start() {
  await initDB();
  app.listen(PORT, () => { console.log(`\n🎺 백엔드 서버 시작 (포트: ${PORT})`); fetchAll(); });
  cron.schedule('0 0 * * *', generateDailySummary, { timezone: "Asia/Seoul" });
  
  // 🔥 429 방지용 60초 주기 (매우 중요)
  setInterval(() => { fetchAll().catch(console.error); }, 900000); // 100초 = 하루 약 864회 × 4서버 
}
start().catch(console.error);