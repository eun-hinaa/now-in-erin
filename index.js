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

// ── DB 초기화 ──
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
  if (/길드원|길원|길드모집|길드 홍보/.test(msg)) return 'guild';
  if (/파티|구함|모집|인원|\/\d|[0-9]\/[0-9]/.test(msg)) return 'party';
  if (/팝니다|팝|판매|삽니다|구매|구입|얼마|골드|가격/.test(msg)) return 'trade';
  return 'etc';
}

const NORMALIZE_MAP = {
  '브리': '브리레흐', '브레': '브리레흐', '1-3관': '브리1-3관', '크롬': '크롬일반', 
  '크일': '크롬일반', '크쉬': '크롬쉬움', '글렌': '글렌일반', '글매': '글렌일반', '뀨': '구슬구매'
};

// ── 넥슨 API 수집 로직 (429 방지 강화) ──
async function fetchServer(serverName) {
  try {
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
    if (newCount > 0) console.log(`[${serverName}] ${newCount}건 저장`);
    return newCount;
  } catch (e) { return 0; }
}

async function fetchAll() {
  for (const server of SERVERS) {
    await fetchServer(server);
    await new Promise(r => setTimeout(r, 1000)); // 서버당 1초 휴식
  }
}

// ── API 엔드포인트 ──

app.get('/api/feed', async (req, res) => {
  const { server, category, keyword, offset } = req.query;
  let query = 'SELECT * FROM horn', params = [], cond = [];
  if (server && server !== 'all') { params.push(server); cond.push(`server_name = $${params.length}`); }
  if (category && category !== 'all') { params.push(category); cond.push(`category = $${params.length}`); }
  if (keyword) { params.push(`%${keyword}%`); cond.push(`(message ILIKE $${params.length} OR character_name ILIKE $${params.length})`); }
  if (cond.length > 0) query += ' WHERE ' + cond.join(' AND ');
  query += ` ORDER BY date_send DESC LIMIT 100 OFFSET ${parseInt(offset) || 0}`;
  const result = await pool.query(query, params);
  res.json({ items: result.rows });
});

app.get('/api/stats/summary', async (req, res) => {
  const total = await pool.query('SELECT COUNT(*) as count FROM horn');
  const today = await pool.query(`SELECT COUNT(*) as count FROM horn WHERE date_send >= NOW() - INTERVAL '24 hours'`);
  const servers = {};
  for (const s of SERVERS) {
    const r = await pool.query('SELECT COUNT(*) as count FROM horn WHERE server_name = $1', [s]);
    servers[s] = parseInt(r.rows[0].count);
  }
  res.json({ total: parseInt(total.rows[0].count), today: parseInt(today.rows[0].count), servers });
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
  let query = `SELECT horn_king_name as name, server_name, COUNT(*) as win_count, SUM(horn_king_count) as total_horns FROM daily_summary WHERE horn_king_name != '없음'`;
  if (server && server !== 'all') query += ` AND server_name = '${server}'`;
  query += ` GROUP BY horn_king_name, server_name ORDER BY win_count DESC LIMIT 10`;
  const result = await pool.query(query);
  res.json(result.rows);
});

app.get('/api/stats/keywords', async (req, res) => {
  const { server, category, days } = req.query;
  const since = new Date(Date.now() - (parseInt(days) || 1) * 24 * 60 * 60 * 1000).toISOString();
  let query = `SELECT character_name, message FROM horn WHERE date_send >= $1`;
  const params = [since];
  if (server && server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }
  if (category && category !== 'all') { params.push(category); query += ` AND category = $${params.length}`; }
  const result = await pool.query(query, params);
  const nicknames = new Set(result.rows.map(r => r.character_name));
  const freq = {};
  result.rows.forEach(r => {
    const words = r.message.split(/[\s\[\]\(\)#:,.!?~ㅋㅎ/\\]+/).filter(w => w.length >= 2);
    words.forEach(w => { if (!nicknames.has(w)) freq[w] = (freq[w] || 0) + 1; });
  });
  res.json({ keywords: Object.entries(freq).sort((a,b) => b[1]-a[1]).slice(0, 20).map(([word, count]) => ({ word, count })) });
});

app.get('/api/stats/guilds', async (req, res) => {
  const { server } = req.query;
  let query = `SELECT server_name, character_name, message, date_send FROM horn WHERE category = 'guild' AND date_send >= NOW() - INTERVAL '3 days'`;
  const params = [];
  if (server && server !== 'all') { params.push(server); query += ` AND server_name = $${params.length}`; }
  query += ` ORDER BY date_send DESC LIMIT 40`;
  const result = await pool.query(query, params);
  res.json(result.rows);
});

app.get('/api/user/:name', async (req, res) => {
  const name = req.params.name;
  const result = await pool.query(`SELECT server_name, message, date_send, category FROM horn WHERE character_name = $1 ORDER BY date_send DESC LIMIT 100`, [name]);
  if (result.rows.length === 0) return res.json({ found: false });
  const rows = result.rows;
  const servers = {}, categories = { party: 0, trade: 0, guild: 0, etc: 0 }, hourMap = new Array(24).fill(0);
  rows.forEach(r => {
    servers[r.server_name] = (servers[r.server_name] || 0) + 1;
    categories[r.category]++;
    hourMap[(new Date(r.date_send).getUTCHours() + 9) % 24]++;
  });
  res.json({ found: true, name, total: rows.length, servers, categories, hourMap, peakHour: hourMap.indexOf(Math.max(...hourMap)), recentMessages: rows.slice(0, 10).map(r => r.message) });
});

app.get('/api/user/:name/analyze', async (req, res) => {
  const name = req.params.name;
  if (!genAI) return res.status(500).json({ error: 'Gemini API 키가 없어요' });
  const result = await pool.query(`SELECT message, date_send FROM horn WHERE character_name = $1 ORDER BY date_send DESC LIMIT 100`, [name]);
  if (result.rows.length === 0) return res.json({ found: false });
  const messages = result.rows.map(r => `[${(new Date(r.date_send).getUTCHours() + 9) % 24}시] ${r.message}`).join('\n');
  const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
  const prompt = `너는 마비노기 20년차 고인물이야. "${name}" 유저 분석 요청:\n${messages}\n🚨 룰: 뉴비 호소는 기만임. 브리=브리 레흐 던전. 뀨=구구=구슬구매(재력가). JSON만 응답: {"type":"칭호", "description":"요약", "traits":["특징"], "activeTime":"시간대", "mainActivity":"주요활동"}`;
  const result2 = await model.generateContent(prompt);
  res.json({ found: true, analysis: JSON.parse(result2.response.text().replace(/```json|```/g, '').trim()) });
});

// ── 자정 정산 로직 ──
async function generateDailySummary() {
  const yesterday = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
  yesterday.setDate(yesterday.getDate() - 1);
  const targetDateStr = yesterday.toISOString().split('T')[0];
  for (const server of SERVERS) {
    const res = await pool.query(`SELECT character_name, COUNT(*) as count FROM horn WHERE server_name = $1 AND DATE(date_send AT TIME ZONE 'Asia/Seoul') = $2 GROUP BY character_name ORDER BY count DESC LIMIT 1`, [server, targetDateStr]);
    if (res.rows.length > 0) {
      await pool.query(`INSERT INTO daily_summary (target_date, server_name, total_messages, peak_hour, popular_dungeon, horn_king_name, horn_king_count) VALUES ($1, $2, 0, 0, '크롬바스', $3, $4) ON CONFLICT DO NOTHING`, [targetDateStr, server, res.rows[0].character_name, res.rows[0].count]);
    }
  }
}

app.get('/api/admin/force-summary', async (req, res) => { await generateDailySummary(); res.send('정산 완료'); });

async function start() {
  await initDB();
  app.listen(PORT, () => { console.log(`🎺 서버 시작: ${PORT}`); fetchAll(); });
  cron.schedule('0 0 * * *', generateDailySummary, { timezone: "Asia/Seoul" });
  setInterval(fetchAll, 60000); // 1분 주기
}
start().catch(console.error);