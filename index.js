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
  if (/길드원|길원/.test(msg)) return 'guild';
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
app.get('/api/feed', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const { category, keyword, server, offset } = req.query;
  let query = 'SELECT * FROM horn', params = [], conditions = [];

  if (server && server !== 'all') { params.push(server); conditions.push(`server_name = $${params.length}`); }
  if (category && category !== 'all') { params.push(category); conditions.push(`category = $${params.length}`); }
  if (keyword) { params.push(`%${keyword}%`); conditions.push(`(message ILIKE $${params.length} OR character_name ILIKE $${params.length})`); }

  if (conditions.length > 0) query += ' WHERE ' + conditions.join(' AND ');
  params.push(limit); params.push(parseInt(offset) || 0);
  query += ` ORDER BY date_send DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

  const result = await pool.query(query, params);
  res.json({ items: result.rows, count: result.rows.length });
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
      const result = await pool.query(`SELECT character_name, COUNT(*) as count FROM horn WHERE server_name = $1 AND date_send::timestamptz >= NOW() - INTERVAL '24 hours' GROUP BY character_name ORDER BY count DESC LIMIT 1`, [server]);
      if (result.rows.length > 0) {
        const name = result.rows[0].character_name;
        kings[server] = { masked: name.length <= 2 ? name[0] + 'X' : name.slice(0, 2) + 'X'.repeat(name.length - 2), count: parseInt(result.rows[0].count) };
      }
    }
    res.json(kings);
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
        VALUES ($1, $2, $3, $4, '크롬바스', $6, $7)
        ON CONFLICT (target_date, server_name) DO UPDATE SET
          total_messages = EXCLUDED.total_messages, peak_hour = EXCLUDED.peak_hour, horn_king_name = EXCLUDED.horn_king_name, horn_king_count = EXCLUDED.horn_king_count
      `, [targetDateStr, server, totalMessages, peakHour, hornKingName, hornKingCount]);
    } catch (err) { console.error(err); }
  }
}

app.get('/api/admin/force-summary', async (req, res) => {
  await generateDailySummary();
  res.send('✅ 어제 데이터 강제 정산 완료! 새로고침 해보세요.');
});

async function start() {
  await initDB();
  app.listen(PORT, () => { console.log(`\n🎺 백엔드 서버 시작 (포트: ${PORT})`); fetchAll(); });
  cron.schedule('0 0 * * *', generateDailySummary, { timezone: "Asia/Seoul" });
  
  // 🔥 429 방지용 60초 주기 (매우 중요)
  setInterval(() => { fetchAll().catch(console.error); }, 60000); 
}
start().catch(console.error);