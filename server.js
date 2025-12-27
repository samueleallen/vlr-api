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
// Fetch all players
app.get('/players', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM Players');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Database error');
  }
});

// Fetch players with filters for region, main agent(s), and 90-Days or All time.
app.get("/players/filtered", async(req, res) => {
  const regionName = req.query.regionName;
  const agentName = req.query.mainAgent;
  const timespan = req.query.timespan;

  try {
    if (regionName != "All") {
      // Get region id
      const regionResult = await pool.query('SELECT region_id FROM Regions WHERE region_name = $1', [regionName]) // regions are constrained to unique names
      if (regionResult.rows.length == 0) {
        return res.status(404).json({error: 'Region not found.'});
      }
      const region_id = regionResult.rows[0].region_id;

      if (agentName != "All") {
        // Get agent id
        const agentResult = await pool.query('SELECT agent_id FROM Agents WHERE agent_name = $1', [agentName]) // regions are constrained to unique names
        if (agentResult.rows.length == 0) {
          return res.status(404).json({error: 'Agent not found.'});
        }
        const agent_id = agentResult.rows[0].agent_id;
        if (timespan == "90_Days") {
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, r2 AS "R2.0", acs AS ACS, kd_ratio AS KD
              FROM PlayerAgentStats_90Days JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE region_id = $1 AND agent_id = $2
              ORDER BY ACS DESC`, [region_id, agent_id]
          );
          res.json(result.rows);
        } else { // Else we fetch all past data, not just 90 days
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, r2 AS "R2.0", acs AS ACS, kd_ratio AS KD
              FROM PlayerAgentStats JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE region_id = $1 AND agent_id = $2
              ORDER BY ACS DESC`, [region_id, agent_id]
          );
          res.json(result.rows);
        }
      } else { // Still using region, but no agents
        if (timespan == "90_Days") {
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, ROUND(AVG(r2), 2) AS "R2.0", ROUND(AVG(acs), 2) AS ACS, ROUND(AVG(kd_ratio), 2) AS KD
              FROM PlayerAgentStats_90Days JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE region_id = $1
              GROUP BY Player, Team
              ORDER BY ACS DESC`, [region_id]
          );
          res.json(result.rows);
        } else { // Else we fetch all past data, not just 90 days
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, ROUND(AVG(r2), 2) AS "R2.0", ROUND(AVG(acs), 2) AS ACS, ROUND(AVG(kd_ratio), 2) AS KD
              FROM PlayerAgentStats JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE region_id = $1
              GROUP BY Player, Team
              ORDER BY ACS DESC`, [region_id]
          );
          res.json(result.rows);
        }
      }
    } else { // Filter over all regions
      if (agentName != "All") {
        // Get agent id
        const agentResult = await pool.query('SELECT agent_id FROM Agents WHERE agent_name = $1', [agentName]) // regions are constrained to unique names
        if (agentResult.rows.length == 0) {
          return res.status(404).json({error: 'Agent not found.'});
        }
        const agent_id = agentResult.rows[0].agent_id;
        if (timespan == "90_Days") {
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, r2 AS "R2.0", acs AS ACS, kd_ratio AS KD
              FROM PlayerAgentStats_90Days JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE agent_id = $1
              ORDER BY ACS DESC`, [agent_id]
          );
          res.json(result.rows);
        } else { // Else we fetch all past data, not just 90 days
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, r2 AS "R2.0", acs AS ACS, kd_ratio AS KD
              FROM PlayerAgentStats JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              WHERE agent_id = $1
              ORDER BY ACS DESC`, [agent_id]
          );
          res.json(result.rows);
        }
      } else { // Still not using region, but no agents
        if (timespan == "90_Days") {
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, ROUND(AVG(r2), 2) AS "R2.0", ROUND(AVG(acs), 2) AS ACS, ROUND(AVG(kd_ratio), 2) AS KD
              FROM PlayerAgentStats_90Days JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              GROUP BY Player, Team
              ORDER BY ACS DESC`
          );
          res.json(result.rows);
        } else { // Else we fetch all past data, not just 90 days
          const result = await pool.query(
            `SELECT player_name AS Player, team_name AS Team, ROUND(AVG(r2), 2) AS "R2.0", ROUND(AVG(acs), 2) AS ACS, ROUND(AVG(kd_ratio), 2) AS KD
              FROM PlayerAgentStats JOIN Roster USING (player_id) JOIN Teams USING (team_id) JOIN Players USING (player_id)
              GROUP BY Player, Team
              ORDER BY ACS DESC`
          );
          res.json(result.rows);
        }
      }
    }
  } catch (err) {
      console.error('Full error:', err);
      console.error('Error message:', err.message);
      res.status(500).json({ 
        error: 'Database error',
        message: err.message,
        code: err.code
      });
  }
});

// ========= End Point for Teams ========= //
// Fetches all teams
app.get('/teams', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM Teams');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Database error');
  }
});

// ========= End Point for Player Data Manager ========= //
// Fetches all players along with their teams
app.get('/admin/players', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT player_name, team_name FROM Players JOIN Roster USING (player_id) JOIN Teams USING (team_id) ORDER BY team_name ASC'
    );
    res.json(result.rows);
  } catch (err) {
    console.error('Full error object:', err);
    console.error('Error message:', err.message);
    console.error('Error code:', err.code);
    res.status(500).json({ 
      error: 'Database error',
      message: err.message,
      code: err.code
    });
  }
});

// Adds a player to the database
app.post('/admin/players', async (req, res) => {
  const {player_ign, team_name} = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Get team id
    const teamResult = await client.query('SELECT team_id FROM TEAMS WHERE team_name = $1', [team_name]) // teams are constrained to unique names
    if (teamResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Team not found.'});
    }
    const team_id = teamResult.rows[0].team_id;

    // Insert player into database and return player id
    const playerInsertResult = await client.query(
      'INSERT INTO Players (player_name) VALUES ($1) RETURNING player_id', [player_ign]
    );
    const player_id = playerInsertResult.rows[0].player_id;

    // Add player to team Roster
    await client.query(
      'INSERT INTO Roster (player_id, team_id) VALUES ($1, $2)', [player_id, team_id]
    );

    await client.query('COMMIT');
    res.status(201).json({message: 'Sucessfully inserted player.', player_id: player_id});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not add player: ', err);
    res.status(500).json({error: 'Failed to insert player.'})
  } finally {
    client.release();
  }
});

// Updates a player in the database
app.put('/admin/players', async(req, res) => {
  const {curr_ign, new_ign, new_team_name} = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Find player id
    const playerResult = await client.query('SELECT player_id FROM Players WHERE player_name = $1', [curr_ign]); // players also constrained to unique names
    if (playerResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Player not found.'});
    }
    const player_id = playerResult.rows[0].player_id

    // Update player name (if new ign is provided)
    if (new_ign && new_ign !== curr_ign) {
      await client.query('UPDATE Players SET player_name = $1 WHERE player_id = $2', [new_ign, player_id]);
    }

    // Update roster (if new team name is provided)
    if (new_team_name) {
      const teamResult = await client.query('SELECT team_id FROM Teams WHERE team_name = $1', [new_team_name]);

      if (teamResult.rows.length == 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({error: `Team '${new_team_name}' not found.`});
      }
      const new_team_id = teamResult.rows[0].team_id;

      await client.query('UPDATE Roster SET team_id = $1 WHERE player_id = $2', [new_team_id, player_id]);
    }

    await client.query('COMMIT');
    res.status(200).json({message: 'Player sucessfully updated.'});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not update player: ', err);
    res.status(500).json({error: 'Failed to update player.'})
  } finally {
    client.release();
  }
});

// Removes a player from the database
app.delete('/admin/players/:name', async(req, res) => {
  const player_ign = req.params.name;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Find player id
    const playerResult = await client.query('SELECT player_id FROM Players WHERE player_name = $1', [player_ign]); // players also constrained to unique names
    if (playerResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Player not found.'});
    }
    const player_id = playerResult.rows[0].player_id

    // Delete from roster, playerAgentStats, and playerAgentStats_90days before deleting from Players relation
    await client.query('DELETE FROM Roster WHERE player_id = $1', [player_id])
    await client.query('DELETE FROM PlayerAgentStats WHERE player_id = $1', [player_id])
    await client.query('DELETE FROM PlayerAgentStats_90Days WHERE player_id = $1', [player_id])

    // Then delete from Players table
    const deleteResult = await client.query('DELETE FROM Players WHERE player_id = $1', [player_id])

    if (deleteResult.rows.Count == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: `Player '${player_ign}' could not be deleted.`});
    }

    await client.query('COMMIT');
    res.status(200).json({message: 'Player sucessfully removed.'});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not remove player: ', err);
    res.status(500).json({error: 'Failed to delete player.'})
  } finally {
    client.release();
  }
});

// ========= End Points for Team Data Manager ========= //

// Fetches all teams along with their regions
app.get('/admin/teams', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT team_name, region_name FROM Regions JOIN Teams USING (region_id) ORDER BY region_name ASC, team_name ASC'
    );
    res.json(result.rows);
  } catch (err) {
    console.error('Full error object:', err);
    console.error('Error message:', err.message);
    console.error('Error code:', err.code);
    res.status(500).json({ 
      error: 'Database error',
      message: err.message,
      code: err.code
    });
  }
});

// Adds a Team to the database
app.post('/admin/teams', async (req, res) => {
  const {region_name, team_name} = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Get region id
    const regionResult = await client.query('SELECT region_id FROM Regions WHERE region_name = $1', [region_name]) // regions are constrained to unique names
    if (regionResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Region not found.'});
    }
    const region_id = regionResult.rows[0].region_id;

    // Insert team into database and return team id
    const teamInsertResult = await client.query(
      'INSERT INTO Teams (team_name, region_id) VALUES ($1, $2) RETURNING team_id', [team_name, region_id]
    );
    const team_id = teamInsertResult.rows[0].team_id;

    await client.query('COMMIT');
    res.status(201).json({message: 'Sucessfully inserted team.', team_id: team_id});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not add team: ', err);
    res.status(500).json({error: 'Failed to insert team.'})
  } finally {
    client.release();
  }
});

// Updates a team in the database
app.put('/admin/teams', async(req, res) => {
  const {new_region, team_name, new_team_name} = req.body;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Find team id
    const teamResult = await client.query('SELECT team_id FROM Teams WHERE team_name = $1', [team_name]); // team names constrained to unique names
    if (teamResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Team not found.'});
    }
    const team_id = teamResult.rows[0].team_id

    // Update team name (if new team name is provided)
    if (new_team_name && new_team_name !== team_name) {
      await client.query('UPDATE Teams SET team_name = $1 WHERE team_id = $2', [new_team_name, team_id]);
    }

    // Update region (if new region is provided)
    if (new_region) {
      const teamResult = await client.query('SELECT region_id FROM Regions WHERE region_name = $1', [new_region]);

      if (teamResult.rows.length == 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({error: `Team '${new_team_name}' not found.`});
      }
      const new_region_id = teamResult.rows[0].region_id;

      await client.query('UPDATE Teams SET region_id = $1 WHERE team_id = $2', [new_region_id, team_id]);
    }

    await client.query('COMMIT');
    res.status(200).json({message: 'Team sucessfully updated.'});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not update team: ', err);
    res.status(500).json({error: 'Failed to update team.'})
  } finally {
    client.release();
  }
});

// Removes a team from the database
app.delete('/admin/teams/:name', async(req, res) => {
  const team_name = req.params.name;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Find team id
    const teamResult = await client.query('SELECT team_id FROM Teams WHERE team_name = $1', [team_name]); // teams also constrained to unique names
    if (teamResult.rows.length == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: 'Team not found.'});
    }
    const team_id = teamResult.rows[0].team_id

    // Delete from Roster, MatchStats, and Matches
    await client.query('DELETE FROM Roster WHERE team_id = $1', [team_id])
    await client.query('DELETE FROM MatchStats WHERE team_id = $1', [team_id])
    await client.query('UPDATE Matches SET winner_team_id = NULL, loser_team_id = NULL WHERE winner_team_id = $1 OR loser_team_id = $1', [team_id]);

    // Then delete from Players table
    const deleteResult = await client.query('DELETE FROM Teams WHERE team_id = $1', [team_id])

    if (deleteResult.rows.Count == 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({error: `Team '${team_name}' could not be deleted.`});
    }

    await client.query('COMMIT');
    res.status(200).json({message: 'Team sucessfully removed.'});
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error: Could not remove team: ', err);
    res.status(500).json({error: 'Failed to delete team.'})
  } finally {
    client.release();
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
            "SELECT agent_name, rounds, r2, use_pct, acs, kd_ratio, adr, kast_pct, kpr, fkpr, kills, deaths, assists, fk, fd FROM PlayerAgentStats JOIN Agents USING (agent_id) WHERE player_id = $1 ORDER BY rounds DESC",
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
});

// ========= End Point for Matches ========= //
// Fetches all matches for a specific team (w/ filters for region and date played)
app.get("/team/matches/filtered", async(req, res) => {
  const {teamName, regionName, win, startDate, endDate} = req.query;
  try {
      // Get team id
      const teamResult = await pool.query(
          'SELECT team_id FROM Teams WHERE team_name = $1',
          [teamName]
      );
      
      // If team DNE, return error
      if (teamResult.rows.length == 0) {
          return res.status(404).json({error: "Team not found"});
      }
      const team_id = teamResult.rows[0].team_id;

      let queryParams = [team_id];
      let queryText = `SELECT t1.team_name AS Home_Team, t2.team_name AS Away_Team, TO_CHAR(m.date_played, 'DD Mon YYYY') AS Date,
                        CASE
                          WHEN m.t1_won IS NULL THEN 'Not Played'
                          WHEN m.t1_won AND m.team1_id = $1 THEN 'WIN'
                          WHEN NOT m.t1_won AND m.team2_id = $1 THEN 'WIN'
                          ELSE 'Loss'
                        END AS Result
                      FROM Matches m JOIN Teams t1 ON (m.team1_id = t1.team_id)
                        JOIN Teams t2 ON (m.team2_id = t2.team_id)
                      WHERE (team1_id = $1 OR team2_id = $1)`;
      
      // Add date filters
      if (startDate) {
        queryParams.push(startDate);
        queryText += ` AND date_played >= $${queryParams.length}`;
      }
      if (endDate) {
        queryParams.push(endDate);
        queryText += ` AND date_played <= $${queryParams.length}`;
      }

      // Add region filter
      if (regionName && regionName != "All") {
        // If 'international', other teams region must be different from their own
        if (regionName == "International") {
          queryText += ` AND (t1.region_id != t2.region_id)`;
        } else { // else other teams region must be the same for 'local'
          queryText += ` AND (t1.region_id = t2.region_id)`;
        }
      }

      // Add win filter
      if (win == "true") {
        queryText += ` AND m.t1_won IS NOT NULL AND ((m.t1_won = true AND m.team1_id = $1) OR (m.t1_won = false AND m.team2_id = $1))`
      }

      queryText += ` Order BY date_played DESC`;
      
      // Fetch matches for team 
      const matchResult = await pool.query(queryText, queryParams);

      // Send back match info
      res.json(matchResult.rows);
  }
  catch (err) {
      console.error(err);
      res.status(500).json({error: "Server error. Could not process request."})
  }
})

app.get("/team/tierlist", async(req, res) => {
  const regionName = req.query.regionName;
  
  try {
    if (regionName != "All") {
      // Get region id
      const regionResult = await pool.query('SELECT region_id FROM Regions WHERE region_name = $1', [regionName]) // regions are constrained to unique names
      if (regionResult.rows.length == 0) {
        return res.status(404).json({error: 'Region not found.'});
      }
      const region_id = regionResult.rows[0].region_id;
      
      // Query to rank teams over the last month based on kd (w/ region filter)
      // Note: Query can be empty if it is ran during the off-season, like in December... Also can be empty
      // if it is ran any time past March of 2025 since last time i ran match webscraper was then...
      // To fix this issue, i just hard-coded January temporarily instead of 'EXTRACT(MONTH FROM date_played) = EXTRACT(MONTH FROM CURRENT_DATE)'
      const result = await pool.query(
        `WITH MonthlyTeamPerformance AS (
          SELECT team_name AS Team, EXTRACT(MONTH FROM date_played) AS Month, ROUND((SUM(kills)/SUM(deaths)), 2) AS Average_KD, ROUND(AVG(acs), 2) AS Average_ACS,
          COUNT(*) AS Total_Matches
          FROM Matches m JOIN MatchStats ms USING (match_id) JOIN Teams t ON (t.team_id = ms.team_id)
          WHERE region_id = $1 AND EXTRACT(YEAR FROM date_played) = EXTRACT(YEAR FROM CURRENT_DATE) AND
            EXTRACT(MONTH FROM date_played) = 1
          GROUP BY Month, t.team_name
        )
        SELECT *, DENSE_RANK() OVER (ORDER BY Average_KD DESC) as Monthly_Ranking
        FROM MonthlyTeamPerformance
        ORDER BY Monthly_Ranking ASC`, [region_id]
      );

      res.json(result.rows);
    } else {
      // Query to rank teams over the last month based on kd (no region filter)
      // Note: Query can be empty if it is ran during the off-season, like in December... Also can be empty
      // if it is ran any time past March of 2025 since last time i ran match webscraper was then...
      // To fix this issue, i just hard-coded January temporarily instead of 'EXTRACT(MONTH FROM date_played) = EXTRACT(MONTH FROM CURRENT_DATE)'
      const result = await pool.query(
        `WITH MonthlyTeamPerformance AS (
          SELECT team_name AS Team, EXTRACT(MONTH FROM date_played) AS Month, ROUND((SUM(kills)/SUM(deaths)), 2) AS Average_KD, ROUND(AVG(acs), 2) AS Average_ACS,
          COUNT(*) AS Total_Matches
          FROM Matches m JOIN MatchStats ms USING (match_id) JOIN Teams t ON (t.team_id = ms.team_id)
          WHERE EXTRACT(YEAR FROM date_played) = EXTRACT(YEAR FROM CURRENT_DATE) AND
            EXTRACT(MONTH FROM date_played) = 1
          GROUP BY Month, t.team_name
        )
        SELECT *, DENSE_RANK() OVER (ORDER BY Average_KD DESC) as Monthly_Ranking
        FROM MonthlyTeamPerformance
        ORDER BY Monthly_Ranking ASC`
      );

      res.json(result.rows);
    }
  } catch (err) {
      console.error('Full error:', err);
      console.error('Error message:', err.message);
      res.status(500).json({ 
        error: 'Database error',
        message: err.message,
        code: err.code
      });
  }
});

// ========= End Point for Agent Tier List ========= //
app.get("/agents/tierlist", async(req, res) => {
  const regionName = req.query.regionName;
  
    try {
      if (regionName != "ALL") {
        // Get region id
        const regionResult = await pool.query('SELECT region_id FROM Regions WHERE region_name = $1', [regionName]) // regions are constrained to unique names
        if (regionResult.rows.length == 0) {
          return res.status(404).json({error: 'Region not found.'});
        }
        const region_id = regionResult.rows[0].region_id;
        
        // Query to rank agents based on kd and acs (w/ region filter)
        const result = await pool.query(
          `WITH AgentPerformance AS (
            SELECT agent_name AS Agent, ROUND(AVG(kd_ratio), 2) AS Average_KD, ROUND(AVG(acs), 2) AS Average_ACS,
            SUM(rounds) AS Total_Rounds
            FROM Agents JOIN PlayerAgentStats USING (agent_id) JOIN Roster USING (player_id) JOIN Teams USING (team_id)
            WHERE region_id = $1
            GROUP BY agent_name
            HAVING SUM(rounds) > 100
          )
          SELECT Agent, Average_KD, Average_ACS, Total_Rounds, DENSE_RANK() OVER (ORDER BY Average_KD
            DESC, Average_ACS DESC) AS Rank
          FROM AgentPerformance
          ORDER BY Rank ASC`, [region_id]);

        res.json(result.rows);
      } else {
        // Query to rank agents based on kd and acs (no region filter)
        const result = await pool.query(
          `WITH AgentPerformance AS (
            SELECT agent_name AS Agent, ROUND(AVG(kd_ratio), 2) AS Average_KD, ROUND(AVG(acs), 2) AS Average_ACS,
            SUM(rounds) AS Total_Rounds
            FROM Agents JOIN PlayerAgentStats USING (agent_id) JOIN Roster USING (player_id) JOIN Teams USING (team_id)
            GROUP BY agent_name
            HAVING SUM(rounds) > 100
          )
          SELECT Agent, Average_KD, Average_ACS, Total_Rounds, DENSE_RANK() OVER (ORDER BY Average_KD
            DESC, Average_ACS DESC) AS Rank
          FROM AgentPerformance
          ORDER BY Rank ASC`
        );

        res.json(result.rows);
      }
  } catch (err) {
    console.error('Full error:', err);
    console.error('Error message:', err.message);
    res.status(500).json({ 
      error: 'Database error',
      message: err.message,
      code: err.code
    });
  }
});

app.listen(port, () => {
  console.log(`API running on port ${port}`);
});