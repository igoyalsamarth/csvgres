import express from 'express';
import { CsvDatabase } from './utils/csvDatabase';

const app = express();
const port = 3000;
const csvDb = new CsvDatabase();

// Move middleware setup before router definition
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Initialize the CSV database
csvDb.init().catch(console.error);

const router = express.Router();

// Add health check route
router.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

router.post('/query', async (req: any, res: any) => {
  try {
    const { query } = req.body;

    if (!query || typeof query !== 'string') {
      return res.status(400).json({
        error: 'Query is required and must be a string'
      });
    }

    // For now, we only handle CREATE TABLE queries
    if (query.toLowerCase().includes('create table')) {
      await csvDb.createTable(query);
      return res.json({
        success: true,
        message: 'Table created successfully'
      });
    } else if (query.toLowerCase().includes('insert into')) {
      await csvDb.insert(query);
      return res.json({
        success: true,
        message: 'Data inserted successfully'
      });
    } else if (query.toLowerCase().includes('select')) {
      const results = await csvDb.select(query);
      return res.json({
        success: true,
        data: results,
        message: 'Data selected successfully'
      });
    }

    return res.status(400).json({
      error: 'Only CREATE TABLE and INSERT INTO queries are supported'
    });
  } catch (error) {
    console.error('Query error:', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Unknown error occurred'
    });
  }
});

// Mount the router
app.use('/', router);

app.listen(port, () => {
  console.log(`Express is listening at http://localhost:${port}`);
  console.log('Available endpoints:');
  console.log('- GET /health');
  console.log('- POST /query');
});