require("dotenv").config();
const express = require("express");
const noblox = require("noblox.js");

const app = express();
app.use(express.json());

const COOKIE = process.env.ROBLOX_COOKIE;
const GROUP_ID = Number(process.env.GROUP_ID);
const API_KEY = process.env.API_KEY;
const PORT = Number(process.env.PORT || 3000);

if (!COOKIE) {
  throw new Error("ROBLOX_COOKIE가 .env에 없습니다.");
}
if (!GROUP_ID) {
  throw new Error("GROUP_ID가 .env에 없습니다.");
}
if (!API_KEY) {
  throw new Error("API_KEY가 .env에 없습니다.");
}

async function init() {
  const current = await noblox.setCookie(COOKIE);
  console.log("Logged into Roblox as", current.UserName);
}

async function getUserIdFromName(username) {
  const userId = await noblox.getIdFromUsername(username);
  return userId;
}

async function resolveRank(rankInput) {
  if (/^\d+$/.test(rankInput)) {
    return Number(rankInput);
  }
  return rankInput;
}

async function changeRank(groupId, userId, change) {
  const result = await noblox.changeRank(groupId, userId, change);
  return result;
}

function checkApiKey(req, res) {
  const auth = req.headers["x-api-key"];
  if (auth !== API_KEY) {
    res.status(401).json({ error: "Unauthorized" });
    return false;
  }
  return true;
}

app.post("/rank", async (req, res) => {
  try {
    if (!checkApiKey(req, res)) return;

    const { username, rank } = req.body;
    if (!username || !rank) {
      return res.status(400).json({ error: "username과 rank가 필요합니다." });
    }

    const userId = await getUserIdFromName(username);
    const rankArg = await resolveRank(String(rank));

    const newRole = await noblox.setRank(GROUP_ID, userId, rankArg);

    res.json({
      success: true,
      userId,
      username,
      rankInput: rank,
      newRole: {
        id: newRole.id,
        name: newRole.name,
        rank: newRole.rank,
      },
    });
  } catch (err) {
    console.error("POST /rank error:", err);
    res.status(500).json({ error: String(err) });
  }
});

app.post("/bulk-promote", async (req, res) => {
  try {
    if (!checkApiKey(req, res)) return;

    const { usernames } = req.body;
    if (!Array.isArray(usernames) || usernames.length === 0) {
      return res.status(400).json({ error: "usernames 배열이 필요합니다." });
    }

    const results = [];
    for (const name of usernames) {
      try {
        const userId = await getUserIdFromName(name);
        const r = await changeRank(GROUP_ID, userId, 1);
        results.push({
          username: name,
          success: true,
          oldRole: r.oldRole,
          newRole: r.newRole,
        });
      } catch (e) {
        console.error("bulk-promote error for", name, e);
        results.push({
          username: name,
          success: false,
          error: String(e),
        });
      }
    }

    res.json({ success: true, results });
  } catch (err) {
    console.error("POST /bulk-promote error:", err);
    res.status(500).json({ error: String(err) });
  }
});

app.post("/bulk-demote", async (req, res) => {
  try {
    if (!checkApiKey(req, res)) return;

    const { usernames } = req.body;
    if (!Array.isArray(usernames) || usernames.length === 0) {
      return res.status(400).json({ error: "usernames 배열이 필요합니다." });
    }

    const results = [];
    for (const name of usernames) {
      try {
        const userId = await getUserIdFromName(name);
        const r = await changeRank(GROUP_ID, userId, -1);
        results.push({
          username: name,
          success: true,
          oldRole: r.oldRole,
          newRole: r.newRole,
        });
      } catch (e) {
        console.error("bulk-demote error for", name, e);
        results.push({
          username: name,
          success: false,
          error: String(e),
        });
      }
    }

    res.json({ success: true, results });
  } catch (err) {
    console.error("POST /bulk-demote error:", err);
    res.status(500).json({ error: String(err) });
  }
});

init().then(() => {
  app.listen(PORT, () => {
    console.log("Rank server listening on port", PORT);
  });
});
