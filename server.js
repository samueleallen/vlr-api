const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const port = process.env.PORT || 3000;
const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: 'db',
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT
});

app.get('/', async (req, res) => {
  res.json({ time: new Date() });
});

// ========= End Point for Players ========= //
app.get('/players', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM Players');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Database error');
  }
});

// ========= End Point for Teams ========= //
app.get('/teams', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM Teams');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Database error');
  }
});

// ========= End Point for Player Stats ========= //
app.get("/player/:name", async(req, res) => {
    const playerName = req.params.name;

    try {
        // Get player id
        const playerResult = await pool.query(
            'SELECT player_id, player_name FROM players WHERE player_name = $1',
            [playerName]
        );
        
        // If player DNE, return error
        if (playerResult.rows.length == 0) {
            return res.status(404).json({error: "Player not found"});
        }

        const player = playerResult.rows[0]

        // Fetch stats for player
        const statsResult = await pool.query(
            "SELECT * FROM PlayerAgentStats WHERE player_id = $1",
            [player.player_id]
        );

        // Send back player info and stats
        res.json({
            player_id: player.player_id,
            player_name: player.player_name,
            stats: statsResult.rows
        });
    }
    catch (err) {
        console.error(err);
        res.status(500).json({error: "Server error. Could not process request."})
    }

})

app.listen(port, () => {
  console.log(`API running on port ${port}`);
});