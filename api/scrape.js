// api/scrape.js
const fetch = require('node-fetch');
const mysql = require('mysql2/promise');
const axios = require('axios');

// Vercel serverless configuration
exports.config = {
  maxDuration: 60 // 60 seconds max duration (Hobby plan limit)
};

// Database configuration using environment variables
const dbConfig = {
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT),
  user: process.env.DB_USER, 
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

// Trigger the revalidation webhooks
async function triggerRevalidate() {
  try {
    const response = await axios.get('https://cinebucket.vercel.app/api/revalidate', {
      params: { secret: 'revalidatedbyjinnah' },
    });
    console.log('Revalidation triggered:', response.data);
  } catch (err) {
    console.error('Failed to revalidate:', err.message);
  }
}

// Get the last processed page and last movie name
async function getScraperState() {
  const connection = await mysql.createConnection(dbConfig);
  try {
    // Create scraper_state table if it doesn't exist
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS scraper_state (
        id INT PRIMARY KEY AUTO_INCREMENT,
        last_page INT NOT NULL DEFAULT 1,
        last_movie_name VARCHAR(255),
        completed BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    
    // Get the latest state
    const [rows] = await connection.execute(`
      SELECT * FROM scraper_state ORDER BY id DESC LIMIT 1
    `);
    
    // If no state exists, create a new one
    if (rows.length === 0) {
      await connection.execute(`
        INSERT INTO scraper_state (last_page, completed) VALUES (1, FALSE)
      `);
      return { lastPage: 1, lastMovieName: null, completed: false };
    }
    
    return {
      lastPage: rows[0].last_page,
      lastMovieName: rows[0].last_movie_name,
      completed: rows[0].completed === 1
    };
  } finally {
    await connection.end();
  }
}

// Update scraper state
async function updateScraperState(lastPage, lastMovieName = null, completed = false) {
  const connection = await mysql.createConnection(dbConfig);
  try {
    await connection.execute(`
      INSERT INTO scraper_state (last_page, last_movie_name, completed) 
      VALUES (?, ?, ?)
    `, [lastPage, lastMovieName, completed]);
  } finally {
    await connection.end();
  }
}

async function dropScraperStateTable() {
  const connection = await mysql.createConnection(dbConfig);
  try {
    await connection.execute(`DROP TABLE IF EXISTS scraper_state`);
    console.log('✅ Dropped scraper_state table');
  } catch (err) {
    console.error('❌ Failed to drop scraper_state table:', err.message);
  } finally {
    await connection.end();
  }
}

// Get the last movie in database for comparison
async function getLastMovieName() {
  const connection = await mysql.createConnection(dbConfig);
  try {
    const [rows] = await connection.execute('SELECT name FROM movies ORDER BY id DESC LIMIT 1');
    return rows.length ? rows[0].name : null;
  } finally {
    await connection.end();
  }
}

async function insertNewMovies(movies) {
  const connection = await mysql.createConnection(dbConfig);
  const insertedMovies = [];

  try {
    for (const movie of movies) {
      try {
        // Insert into movies
        const [result] = await connection.execute(`
          INSERT INTO movies (name, description, duration, quality, rating, release_date, language, iframe_src, poster, poster_alt, url, year, backdrop_path)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, [
          movie.name,
          movie.description,
          movie.duration?.startsWith('Duration:') ? movie.duration.replace(/^Duration:\s*/, '') : movie.duration,
          movie.quality,
          movie.rating,
          new Date(movie.release_date),
          movie.language,
          movie.iframe_src,
          movie.poster,
          movie.poster_alt,
          movie.url,
          movie.year,
          movie.backdrop_path
        ]);

        const movieId = result.insertId;

        // Insert download links
        for (const link of movie.download_links) {
          await connection.execute(`
            INSERT INTO download_links (movie_id, label, url)
            VALUES (?, ?, ?)
          `, [movieId, link.label, link.url]);
        }

        // Insert genres
        for (const genre of movie.genre || []) {
          const [rows] = await connection.execute(`SELECT id FROM genres WHERE name = ?`, [genre]);
          let genreId = rows[0]?.id;

          if (!genreId) {
            const [genreResult] = await connection.execute(`INSERT INTO genres (name) VALUES (?)`, [genre]);
            genreId = genreResult.insertId;
          }

          await connection.execute(`
            INSERT INTO movie_genres (movie_id, genre_id)
            VALUES (?, ?)
          `, [movieId, genreId]);
        }

        // Insert tags
        for (const tag of movie.tags || []) {
          const [rows] = await connection.execute(`SELECT id FROM tags WHERE name = ?`, [tag]);
          let tagId = rows[0]?.id;

          if (!tagId) {
            const [tagResult] = await connection.execute(`INSERT INTO tags (name) VALUES (?)`, [tag]);
            tagId = tagResult.insertId;
          }

          await connection.execute(`
            INSERT INTO movie_tags (movie_id, tag_id)
            VALUES (?, ?)
          `, [movieId, tagId]);
        }

        insertedMovies.push(movie.name);
        console.log(`✅ Inserted: ${movie.name}`);
        await triggerRevalidate();
      } catch (err) {
        console.error(`❌ Failed to insert movie "${movie.name}":`, err.message);
      }
    }
  } finally {
    await connection.end();
  }
  
  return insertedMovies;
}

async function scrapeMoviesIncrementally() {
  const bearerToken = process.env.SCRAPER_API_TOKEN;
  const dbLastMovieName = await getLastMovieName();
  const state = await getScraperState();
  
  // If the previous run was completed, start a new run
  if (state.completed) {
    await updateScraperState(1, null, false);
    state.lastPage = 1;
    state.lastMovieName = null;
    state.completed = false;
  }
  
  let page = state.lastPage;
  let foundLastMovie = false;
  let newMovies = [];
  let stats = { processedPages: 0, moviesFound: 0, moviesInserted: 0 };
  const maxPagesToProcess = 4; // Process limited pages per run to stay within 60-second limit
  
  try {
    // Process a limited number of pages per execution
    for (let i = 0; i < maxPagesToProcess; i++) {
      console.log(`Processing page ${page}...`);
      
      const response = await fetch('https://mkvking-scraper.vercel.app/api/movies', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${bearerToken}`
        },
        body: JSON.stringify({ page })
      });

      if (!response.ok) {
        console.error(`Failed to fetch page ${page}: ${response.status}`);
        break;
      }

      const data = await response.json();
      if (!data.movies || !Array.isArray(data.movies) || data.movies.length === 0) {
        // No more movies, mark as completed
        await updateScraperState(page, state.lastMovieName, true);
        break;
      }

      stats.processedPages++;
      stats.moviesFound += data.movies.length;
      
      // Collect new movies until we find one that's already in our database
      let moviesForThisPage = [];
      for (const movie of data.movies) {
        if (movie.name === dbLastMovieName) {
          foundLastMovie = true;
          break;
        }
        moviesForThisPage.push(movie);
      }
      
      // If we found the last movie, we've caught up
      if (foundLastMovie) {
        newMovies = [...newMovies, ...moviesForThisPage];
        await updateScraperState(page, moviesForThisPage[0]?.name || state.lastMovieName, true);
        break;
      } else {
        newMovies = [...newMovies, ...moviesForThisPage];
        page++;
        // Save our progress
        await updateScraperState(page, moviesForThisPage[0]?.name || state.lastMovieName, false);
      }
    }
    
    // Insert all found movies (oldest first)
    if (newMovies.length > 0) {
      const insertedMovies = await insertNewMovies(newMovies.reverse());
      stats.moviesInserted = insertedMovies.length;
    }

    if (foundLastMovie) {
      await dropScraperStateTable();
    }
    
    return {
      success: true,
      stats,
      completed: foundLastMovie,
      nextPage: page,
      message: foundLastMovie 
        ? `Completed: Found and processed ${stats.moviesInserted} new movies. Dropped scraper_state table.` 
        : `In progress: Processed ${stats.processedPages} pages, inserted ${stats.moviesInserted} new movies, will continue from page ${page} next run`
    };
    
  } catch (error) {
    console.error('Error during incremental scraping:', error);
    // Save the current state to resume later
    await updateScraperState(page, state.lastMovieName, false);
    
    throw error;
  }
}

module.exports = async function handler(req, res) {
  // Only process GET requests or cron job requests
  if (req.method !== 'GET' && !req.headers['x-vercel-cron']) {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const result = await scrapeMoviesIncrementally();
    return res.status(200).json(result);
  } catch (error) {
    console.error('Scraper error:', error);
    return res.status(500).json({ 
      error: 'Scraper failed', 
      message: error.message 
    });
  }
};
