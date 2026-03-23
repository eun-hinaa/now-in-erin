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

console.log(`[디버그] API_KEY 앞 10자: ${API_KEY ? API_KEY.slice(0, 10) : '없음 (!!)'}`);
console.log(`[디버그] 수집 서버: ${SERVERS.join(', ')}`);

// PostgreSQL 연결
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// DB 초기화
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS horn (
      id SERIAL PRIMARY KEY,
      server_name TEXT,
      character_name TEXT,
      message TEXT,
      date_send TEXT,
      category TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(server_name, character_name, message, date_send)
    );
    CREATE INDEX IF NOT EXISTS idx_date_send ON horn(date_send);
    CREATE INDEX IF NOT EXISTS idx_category ON horn(category);
    CREATE INDEX IF NOT EXISTS idx_server ON horn(server_name);
  `);
  console.log('[DB] 테이블 초기화 완료');
}

// 카테고리 분류
function classify(msg) {
  if (/길드원|길원/.test(msg)) return 'guild';
  if (/파티|구함|모집|인원|\/\d|[0-9]\/[0-9]/.test(msg)) return 'party';
  if (/팝니다|팝|판매|삽니다|구매|구입|얼마|골드|가격/.test(msg)) return 'trade';
  return 'etc';
}

// ── 마비노기 약어 정규화 테이블
const NORMALIZE_MAP = {
  '1채': '채널1', '2채': '채널2', '3채': '채널3', '4채': '채널4',
  '5채': '채널5', '6채': '채널6', '7채': '채널7', '8채': '채널8',
  '9채': '채널9', '10채': '채널10',
  '브리': '브리레흐', '브레': '브리레흐',
  '1관': '브리1-3관', '2관': '브리1-3관', '3관': '브리1-3관',
  '1-3관': '브리1-3관', '4관': '브리4관',
  '브트팟': '브리트라이팟', '브리트팟': '브리트라이팟',
  '구구': '구슬구매', '정코억분': '정코억분배', '정코분배': '정코억분배',
  '크롬': '크롬일반', '크일': '크롬일반', '크롬일': '크롬일반',
  '크쉬': '크롬쉬움', '크롬쉬': '크롬쉬움',
  '상독화분': '상독화분배',
  '글렌': '글렌일반', '글매': '글렌일반', '글렴': '글렌일반',
  '매어': '글렌일반', '글쉬': '글렌쉬움', '글렌매': '글렌일반',
  '독식': '독식분배', '올독식': '올독식분배', '헤분': '헤일로분배',
  '엘나': '엘레멘탈나이트', '세바': '세인트바드', '닼메': '다크메이지',
  '알스': '알케믹스팅어', '세가': '세이크리드가드', '거너': '배리어블거너',
  '포알': '포비든알케미스트', '멜퍼': '펠로딕퍼피티어',
  '퓨파': '퓨리파이터', '뜌따': '퓨리파이터',
  '풀팟': '풀파티', '풀파': '풀파티',
  '중탈': '중도탈주가능',
};

// 불용어
const STOP_WORDS = new Set([
  '이', '가', '은', '는', '을', '를', '에', '의', '와', '과', '도', '로', '으로',
  '에서', '하고', '이고', '그', '저', '것', '수', '있', '없',
  '합니다', '합니다.', '해요', '해요.', '임', '임.', '입니다', '입니다.',
  '모집합니다', '모집해요', '모집중', '모집', '구합니다', '구해요', '구인',
  '있습니다', '없습니다', '됩니다', '드립니다', '드려요',
  '가능', '가능합니다', '가능해요', '불가', '환영', '환영합니다',
  '부탁', '부탁드립니다', '부탁해요', '감사합니다', '감사',
  '참여', '참여해요', '출발', '출발합니다',
  '합시다', '하실', '하세요', '해주세요',
  '같이', '같이요', '같이해요', '같이하실분',
  '하실분', '분들', '분이요', '분만',
  '지금', '바로', '현재', '오늘', '오후', '오전', '저녁', '새벽', '밤', '낮',
  '시', '분', '초', '00시', '01시', '02시', '03시', '04시', '05시', '06시',
  '07시', '08시', '09시', '10시', '11시', '12시', '13시', '14시', '15시',
  '16시', '17시', '18시', '19시', '20시', '21시', '22시', '23시',
]);

function normalizeWord(w) {
  return NORMALIZE_MAP[w] || w;
}

// 단일 서버 수집
async function fetchServer(serverName) {
  try {
    const url = `https://open.api.nexon.com/mabinogi/v1/horn-bugle-world/history?server_name=${encodeURIComponent(serverName)}`;
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
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (server_name, character_name, message, date_send) DO NOTHING`,
          [serverName, item.character_name, item.message, item.date_send, classify(item.message)]
        );
        if (result.rowCount > 0) newCount++;
      } catch (e) {
        // 중복 무시
      }
    }

    console.log(`[${serverName}] ${items.length}건 조회, ${newCount}건 신규 저장`);
    return newCount;
  } catch (e) {
    console.error(`[${serverName}] 오류:`, e.message);
    return 0;
  }
}

// 전 서버 수집
async function fetchAll() {
  const now = new Date().toLocaleTimeString('ko-KR');
  console.log(`\n[${now}] 전 서버 수집 시작...`);
  for (const server of SERVERS) {
    await fetchServer(server);
    await new Promise(r => setTimeout(r, 500));
  }
  console.log(`[${now}] 전 서버 수집 완료\n`);
}

// ─── API 엔드포인트 ───────────────────────────────

// 최근 메시지 피드
app.get('/api/feed', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const category = req.query.category;
  const keyword = req.query.keyword;
  const server = req.query.server;

  let query = 'SELECT * FROM horn';
  const params = [];
  const conditions = [];

  if (server && server !== 'all') {
    params.push(server);
    conditions.push(`server_name = $${params.length}`);
  }
  if (category && category !== 'all') {
    params.push(category);
    conditions.push(`category = $${params.length}`);
  }
  if (keyword) {
    params.push(`%${keyword}%`);
    conditions.push(`(message ILIKE $${params.length} OR character_name ILIKE $${params.length})`);
  }

  if (conditions.length > 0) query += ' WHERE ' + conditions.join(' AND ');
  params.push(limit);
  params.push(parseInt(req.query.offset) || 0);
  query += ` ORDER BY date_send DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

  const result = await pool.query(query, params);
  res.json({ items: result.rows, count: result.rows.length });
});

// 시간대별 활동량
app.get('/api/stats/hourly', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const server = req.query.server;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  let query = `
    SELECT
      MOD(CAST(EXTRACT(HOUR FROM CAST(date_send AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'Asia/Seoul') AS INTEGER), 24) as hour,
      COUNT(*) as count
    FROM horn
    WHERE date_send >= $1
  `;
  const params = [since];

  if (server && server !== 'all') {
    params.push(server);
    query += ` AND server_name = $${params.length}`;
  }

  query += ' GROUP BY hour ORDER BY hour';

  const result = await pool.query(query, params);
  const hourly = new Array(24).fill(0);
  result.rows.forEach(r => { hourly[parseInt(r.hour)] = parseInt(r.count); });

  res.json({ hourly, since, days, server: server || 'all' });
});

// 카테고리 분포
app.get('/api/stats/category', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const server = req.query.server;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  let query = `SELECT category, COUNT(*) as count FROM horn WHERE date_send >= $1`;
  const params = [since];

  if (server && server !== 'all') {
    params.push(server);
    query += ` AND server_name = $${params.length}`;
  }

  query += ' GROUP BY category';
  const result = await pool.query(query, params);
  const out = { party: 0, trade: 0, guild: 0, etc: 0 };
  result.rows.forEach(r => { out[r.category] = parseInt(r.count); });

  res.json(out);
});

// 키워드 트렌드
app.get('/api/stats/keywords', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const server = req.query.server;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  let query = `SELECT character_name, message FROM horn WHERE date_send >= $1`;
  const params = [since];

  if (server && server !== 'all') {
    params.push(server);
    query += ` AND server_name = $${params.length}`;
  }

  const result = await pool.query(query, params);
  const nicknames = new Set(result.rows.map(r => r.character_name));
  const freq = {};

  result.rows.forEach(({ message }) => {
    const normalized = message
      .replace(/(\d{1,2})(채널|채)/g, '채널$1')
      .replace(/채널(\d{1,2})/g, '채널$1');

    const words = normalized
      .split(/[\s\[\]\(\)#:,.!?~ㅋㅎ/\\]+/)
      .map(w => w.trim())
      .filter(w => w.length >= 2);

    words.forEach(raw => {
      const w = normalizeWord(raw);
      if (STOP_WORDS.has(w) || STOP_WORDS.has(raw)) return;
      if (nicknames.has(w) || nicknames.has(raw)) return;
      if (/^[0-9]+$/.test(w)) return;
      if (/^[a-zA-Z]{1,2}$/.test(w)) return;
      // 채널 표기 제거 (채널1, 채널7 등)
      if (/^채널\d+$/.test(w)) return;
      // 사람 수 패턴 제거 (1/4, 3명, 2자 등)
      if (/^[0-9]+\/[0-9]+$/.test(raw)) return;
      if (/^[0-9]+[명자인]$/.test(raw)) return;
      if (/^[0-9]+\/[0-9]+[명]?$/.test(raw)) return;
      freq[w] = (freq[w] || 0) + 1;
    });
  });

  const sorted = Object.entries(freq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([word, count]) => ({ word, count }));

  res.json({ keywords: sorted });
});

// 파티 모집 현황
app.get('/api/stats/party', async (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const server = req.query.server;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  let query = `SELECT message, date_send FROM horn WHERE category = 'party' AND date_send >= $1`;
  const params = [since];

  if (server && server !== 'all') {
    params.push(server);
    query += ` AND server_name = $${params.length}`;
  }

  const result = await pool.query(query, params);
  const rows = result.rows.filter(r => !/길드원|길원/.test(r.message));

  const dungeons = {
    '브리레흐 (1-3관)': { keywords: ['브리레흐', '브리', '브레', '1관', '2관', '3관', '1-3관', '브트팟', '브리트팟', '정코억분', '정코분배', '구구', '구슬구매'], count: 0, recent: [] },
    '브리레흐 (4관)': { keywords: ['4관'], count: 0, recent: [] },
    '크롬바스 일반': { keywords: ['크롬일반', '크일', '크롬일', '크롬바스', '크롬', '상독화분', '100버', '90버'], count: 0, recent: [] },
    '크롬바스 쉬움': { keywords: ['크쉬', '크롬쉬', '크롬쉬움'], count: 0, recent: [] },
    '몽환의 라비': { keywords: ['몽라', '몽몽라', '몽환라비', '몽환의라비', '몽환의 라비'], count: 0, recent: [] },
    '글렌베르나 일반': { keywords: ['글매', '글렴', '글렌일반', '글렌', '글렌베르나', '헤분', '독식', '올독식', '매어'], count: 0, recent: [] },
    '글렌베르나 쉬움': { keywords: ['글쉬', '글렌쉬움'], count: 0, recent: [] },
    '기타': { keywords: [], count: 0, recent: [] },
  };

  rows.forEach(({ message, date_send }) => {
    let normalizedMsg = message;
    for (const [abbr, full] of Object.entries(NORMALIZE_MAP)) {
      normalizedMsg = normalizedMsg.replace(new RegExp(abbr, 'g'), full);
    }

    // 크롬 키워드 우선 체크 (올독식 등 글렌 키워드와 혼용되는 경우 방지)
    const chromePriority = ['크롬바스', '크롬일반', '크일', '크롬일', '크롬', '크쉬', '크롬쉬'];
    const hasChrome = chromePriority.some(kw => message.includes(kw) || normalizedMsg.includes(kw));

    let matched = false;
    for (const [name, info] of Object.entries(dungeons)) {
      if (name === '기타') continue;
      // 크롬 키워드가 있으면 글렌 건너뜀
      if (hasChrome && (name === '글렌베르나 일반' || name === '글렌베르나 쉬움')) continue;
      if (info.keywords.some(kw => message.includes(kw) || normalizedMsg.includes(kw))) {
        info.count++;
        // recent에 중복 메시지 제외
        if (info.recent.length < 5 && !info.recent.some(r => r.message === message)) {
          info.recent.push({ message, date_send });
        }
        matched = true;
        break;
      }
    }
    if (!matched) {
      dungeons['기타'].count++;
      if (dungeons['기타'].recent.length < 5 && !dungeons['기타'].recent.some(r => r.message === message)) {
        dungeons['기타'].recent.push({ message, date_send });
      }
    }
  });

  const memberPatterns = { '1인': 0, '2인': 0, '3인': 0, '4인': 0, '5인': 0, '6인': 0, '7인': 0, '풀파티': 0 };
  rows.forEach(({ message }) => {
    if (/풀팟|풀파/.test(message)) { memberPatterns['풀파티']++; return; }
    const m = message.match(/([1-8])\s*\/\s*[1-8]/) || message.match(/([1-8])인/);
    if (m) {
      const key = `${m[1]}인`;
      if (memberPatterns[key] !== undefined) memberPatterns[key]++;
    }
  });

  const dungeonList = Object.entries(dungeons)
    .map(([name, info]) => ({ name, count: info.count, recent: info.recent }))
    .sort((a, b) => b.count - a.count);

  res.json({ total: rows.length, dungeons: dungeonList, memberPatterns });
});

// 거뿔 왕 (서버별 오늘 최다 발송자)
app.get('/api/stats/horn-king', async (req, res) => {
  try {
    const kings = {};
    for (const server of SERVERS) {
      const result = await pool.query(`
        SELECT character_name, COUNT(*) as count
        FROM horn
        WHERE server_name = $1
          AND date_send::timestamptz >= NOW() - INTERVAL '24 hours'
        GROUP BY character_name
        ORDER BY count DESC
        LIMIT 1
      `, [server]);

      if (result.rows.length > 0) {
        const name = result.rows[0].character_name;
        const count = parseInt(result.rows[0].count);
        // 이름 앞 두 글자만 노출, 나머지 XX
        const masked = name.length <= 2
          ? name[0] + 'X'
          : name.slice(0, 2) + 'X'.repeat(name.length - 2);
        kings[server] = { masked, count };
      } else {
        kings[server] = null;
      }
    }
    res.json(kings);
  } catch (e) {
    console.error('[거뿔 왕 오류]', e.message);
    res.status(500).json({ error: e.message });
  }
});


// 닉네임 검색
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
    if (rows.length === 0) {
      return res.json({ found: false, name });
    }

    // 통계
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
      found: true,
      name,
      total,
      servers,
      categories,
      hourMap,
      peakHour,
      recentMessages,
      oldest: rows[rows.length - 1].date_send,
      newest: rows[0].date_send,
    });
  } catch (e) {
    console.error('[닉네임 검색 오류]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// AI 거뿔 패턴 분석
app.get('/api/user/:name/analyze', async (req, res) => {
  const name = req.params.name;

  if (!genAI) {
    return res.status(500).json({ error: 'Gemini API 키가 없어요' });
  }

  try {
    const result = await pool.query(`
      SELECT message, category, date_send
      FROM horn
      WHERE character_name = $1
      ORDER BY date_send DESC
      LIMIT 100
    `, [name]);

    const rows = result.rows;
    if (rows.length === 0) {
      return res.json({ found: false });
    }

    const messages = rows.map(r => r.message).join('\n');
const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash-latest' });

    const prompt = `다음은 마비노기 게임의 "${name}" 유저가 거대한 외침의 뿔피리(서버 전체 채팅)로 보낸 메시지들이야:

${messages}

이 유저의 거뿔 패턴을 분석해서 아래 JSON 형식으로만 답해줘. 다른 말은 하지 마:
{
  "type": "유저 유형 (예: 브리 파티장, 새벽 사냥꾼, 거래의 신, 뻘뿔러, 길드 홍보대사 등 창의적으로)",
  "description": "2~3문장으로 이 유저 특징 설명 (마비노기 게임 맥락으로)",
  "traits": ["특징1", "특징2", "특징3"],
  "activeTime": "주로 활동하는 시간대",
  "mainActivity": "주요 활동"
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

// 전체 통계 요약
app.get('/api/stats/summary', async (req, res) => {
  const total = await pool.query('SELECT COUNT(*) as count FROM horn');
  const today = await pool.query(`SELECT COUNT(*) as count FROM horn WHERE date_send::timestamptz >= NOW() - INTERVAL '24 hours'`);
  const oldest = await pool.query('SELECT MIN(date_send) as d FROM horn');
  const newest = await pool.query('SELECT MAX(date_send) as d FROM horn');

  const serverCounts = {};
  for (const s of SERVERS) {
    const r = await pool.query('SELECT COUNT(*) as count FROM horn WHERE server_name = $1', [s]);
    serverCounts[s] = parseInt(r.rows[0].count);
  }

  res.json({
    total: parseInt(total.rows[0].count),
    today: parseInt(today.rows[0].count),
    oldest: oldest.rows[0].d,
    newest: newest.rows[0].d,
    servers: serverCounts
  });
});

// ─── 서버 시작 ───────────────────────────────────

async function start() {
  await initDB();

  app.listen(PORT, () => {
    console.log(`\n🎺 지금 에린에서는 — 백엔드 서버 시작`);
    console.log(`   포트: ${PORT}`);
    console.log(`   수집 서버: ${SERVERS.join(', ')}`);
    console.log(`   http://localhost:${PORT}\n`);
    fetchAll();
  });

  cron.schedule('*/10 * * * *', fetchAll);
  console.log('⏰ 10분마다 자동 수집 스케줄 등록 완료');
}

start().catch(console.error);
