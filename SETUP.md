# Project Setup Guide

## Initial Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd datafox_sl
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup configuration**
   ```bash
   cp config.example.json config.json
   ```
   Edit `config.json` with your specific paths and settings.

5. **Create data directories**
   ```bash
   mkdir -p data/ozon data/wb/products data/custom_reports
   ```

6. **Run the application**
   ```bash
   streamlit run main_app.py
   ```

## Project Structure

- `data/` - Database and data files (ignored by git)
- `marketplace_reports/` - Report files (ignored by git)
- `pages/` - Streamlit application pages
- `utils/` - Utility modules and helpers
- `project-docs/` - Project documentation
- `config.json` - Configuration file (ignored by git)

## Important Notes

- Never commit actual data files or configuration with sensitive information
- Use `config.example.json` as a template for new environments
- The `.gitignore` file is configured to protect sensitive marketplace data
- Database files are automatically excluded from version control

## Development

- Follow PEP 8 coding standards
- Add comments for complex logic
- Update documentation in `project-docs/` when adding features
- Test with sample data before using real marketplace data 