# WikiWatch 📊

A real-time Wikipedia analytics dashboard that monitors live Wikipedia edit streams and provides insights into editing patterns, spike detection, and cross-language page analysis.

## 🚀 Features

- **Real-time Data Ingestion**: Streams live Wikipedia edit events from Wikimedia's recent changes feed
- **Interactive Dashboard**: Streamlit-based web interface with auto-refreshing charts and tables
- **Edit Analytics**: Tracks edits per minute by Wikipedia project (language/domain)
- **Top Pages Monitoring**: Identifies most frequently edited pages in sliding time windows
- **Spike Detection**: Automatically detects unusual editing activity spikes (1-minute vs 10-minute baseline)
- **Cross-language Analysis**: Identifies pages being edited across multiple language versions simultaneously

## 🏗️ Architecture

The project follows a bronze-silver-gold data architecture pattern:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bronze Layer  │    │  Processing     │    │   Gold Layer    │
│                 │    │                 │    │                 │
│ Raw JSONL files │───▶│  Apache Spark   │───▶│  Parquet files  │
│ from Wikimedia  │    │  Streaming      │    │  for dashboard  │
│ Recent Changes  │    │  Analytics      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

1. **Ingestion** (`ingest/ingest_to_files.py`): Connects to Wikimedia's Server-Sent Events stream and saves raw edit events as JSONL files
2. **Processing** (`spark/spark_wiki_file.py`): Apache Spark streaming job processes the raw data into structured analytics
3. **Visualization** (`dashboard/app.py`): Streamlit dashboard displays real-time analytics with auto-refresh

## 📊 Analytics
<img width="1331" height="645" alt="Screenshot 2025-10-11 at 16 17 26" src="https://github.com/user-attachments/assets/754998ee-01a9-4954-9e54-52d364b077ac" />


<img width="1306" height="655" alt="Screenshot 2025-10-11 at 16 17 52" src="https://github.com/user-attachments/assets/f8c1ac5e-7d1b-4e0a-88e0-b536d4b449e1" />

### 1. Edits per Minute by Project
- Tumbling 1-minute windows
- Aggregates edit counts by Wikipedia project (language/domain)
- Tracks unique pages edited and bot activity share

### 2. Top Pages
- Sliding 10-minute windows with 1-minute steps
- Ranks pages by edit frequency
- Shows most active pages in recent time periods

### 3. Spike Detection
- Compares 1-minute edit counts to 10-minute rolling average
- Identifies projects with ≥2x normal activity
- Filters for significant spikes (≥5 edits in 10-minute baseline)

### 4. Cross-language Pages
- Identifies pages edited across multiple language versions
- 10-minute sliding windows
- Highlights coordinated editing activity

## 🛠️ Installation

### Prerequisites

- Python 3.12+
- Java 8+ (for Apache Spark)
- Virtual environment (recommended)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd wikiwatch
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   
   Or install manually:
   ```bash
   pip install streamlit pandas pyspark requests sseclient-py pyarrow
   ```

4. **Create data directories**
   ```bash
   mkdir -p data/{bronze,gold,checkpoints}
   ```

## 🚀 Usage

### 1. Start Data Ingestion

The ingestion script connects to Wikimedia's live stream and saves raw edit events:

```bash
python ingest/ingest_to_files.py
```

**Environment Variables:**
- `OUT_DIR`: Output directory for JSONL files (default: `data/bronze`)
- `BATCH_SIZE`: Number of events per file (default: 50)

### 2. Start Spark Processing

The Spark streaming job processes raw data into analytics:

```bash
python spark/spark_wiki_file.py
```

This will:
- Read JSONL files from `data/bronze/`
- Process them through Spark Structured Streaming
- Write results to `data/gold/` as Parquet files
- Maintain checkpoints in `data/checkpoints/`

### 3. Launch Dashboard

Start the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```

The dashboard will be available at `http://localhost:8501` with:
- Auto-refreshing charts and tables
- Configurable refresh intervals
- Real-time edit analytics
- Spike alerts and cross-language page monitoring

## 📁 Project Structure

```
wikiwatch/
├── dashboard/
│   └── app.py              # Streamlit dashboard
├── ingest/
│   └── ingest_to_files.py  # Wikimedia stream ingestion
├── spark/
│   └── spark_wiki_file.py  # Spark streaming analytics
├── data/
│   ├── bronze/             # Raw JSONL files
│   ├── gold/               # Processed Parquet files
│   └── checkpoints/        # Spark streaming checkpoints
└── README.md
```

## 🔧 Configuration

### Dashboard Settings
- **Auto-refresh**: 2-30 seconds (configurable)
- **Display rows**: 10-200 rows per table
- **Time windows**: Configurable lookback periods

### Spark Configuration
- **Shuffle partitions**: 4 (adjust based on cluster size)
- **Watermark**: 20 minutes (for late data handling)
- **Checkpoint locations**: Separate per analytics stream

### Ingestion Settings
- **Batch size**: 50 events per file (configurable)
- **Retry logic**: Exponential backoff with jitter
- **Rate limiting**: Respects Wikimedia's Retry-After headers

## 📈 Data Schema

### Raw Events (Bronze)
```json
{
  "meta": {"dt": "2024-01-01T12:00:00Z", "uri": "..."},
  "server_name": "en.wikipedia.org",
  "wiki": "enwiki",
  "title": "Page Title",
  "user": "username",
  "bot": false,
  "minor": false,
  "type": "edit",
  "length": {"old": 1000, "new": 1050},
  "comment": "Edit summary",
  "namespace": 0
}
```

### Processed Analytics (Gold)
- **by_project**: Edits per minute by Wikipedia project
- **top_pages**: Most edited pages in sliding windows
- **spikes_v2**: Spike detection alerts with ratios
- **crosslang_v2**: Cross-language page coordination

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is for educational purposes. Please respect Wikimedia's terms of service and rate limiting policies.

## 🙏 Acknowledgments

- **Wikimedia Foundation** for providing the Recent Changes stream
- **Apache Spark** for stream processing capabilities
- **Streamlit** for the interactive dashboard framework

## 📞 Support

For questions or issues:
- Check the logs in the terminal where you're running the ingestion/processing
- Verify that Java is installed and accessible
- Ensure all dependencies are installed correctly
- Check that data directories have proper write permissions

---

**Note**: This is a real-time system that continuously processes live Wikipedia data. Make sure you have sufficient disk space for storing the bronze layer files and gold layer analytics.
