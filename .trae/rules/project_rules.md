# AI Rules for Python Development

## Streamlit Best Practices

**Description:** Best practices and guidelines for Streamlit applications  
**Files:** `**/*.py`

- Use caching to optimize performance with `@st.cache_data` and `@st.cache_resource`
- Organize your app into functions for better readability and maintainability
- Utilize session state to manage user inputs and app state effectively
- Implement responsive layouts using columns and containers

## Pandas Best Practices

**Description:** Best practices for data manipulation and analysis with Pandas  
**Files:** `**/*.py`

- Use vectorized operations instead of loops for better performance
- Always check for missing values and handle them appropriately
- Use `DataFrame` methods like `apply()` and `agg()` for efficient data transformations
- Document your data processing steps for reproducibility

## OpenPyXL Best Practices

**Description:** Guidelines for working with Excel files using OpenPyXL  
**Files:** `**/*.py`

- Use context managers to handle file operations safely
- Optimize performance by minimizing the number of read/write operations
- Use styles and formatting judiciously to enhance readability without bloating the file size
- Validate data before writing to Excel to prevent errors

## Pillow Best Practices

**Description:** Best practices for image processing with Pillow  
**Files:** `**/*.py`

- Use `Image.open()` in a context manager to ensure proper resource management
- Optimize image size and format for web applications to improve load times
- Utilize built-in filters and transformations for common image processing tasks
- Always handle exceptions when dealing with file I/O operations

## Plotly Best Practices

**Description:** Best practices for data visualization with Plotly  
**Files:** `**/*.py`

- Use `plotly.express` for quick and easy visualizations with less code
- Keep visualizations simple and avoid clutter for better user comprehension
- Utilize callbacks for interactive features in Dash applications
- Document your visualizations to explain the data and insights clearly

## Requests Best Practices

**Description:** Best practices for making HTTP requests with Requests  
**Files:** `**/*.py`

- Use session objects to persist parameters across requests for better performance
- Handle exceptions and errors gracefully with try-except blocks
- Always validate and sanitize user inputs when constructing URLs
- Use timeouts to prevent hanging requests and improve user experience

## DuckDB Best Practices

**Description:** Best practices for using DuckDB for data analytics  
**Files:** `**/*.py`

- Use in-memory databases for faster query performance during analysis
- Leverage DuckDB's SQL capabilities for complex data manipulations
- Optimize queries by using appropriate indexing and partitioning strategies
- Document your SQL queries for clarity and future reference

## Streamlit Extras Best Practices
 
**Description:** Best practices for using Streamlit Extras  
**Files:** `**/*.py`

- Utilize additional components to enhance user experience and interactivity
- Keep the UI clean by only using necessary extras to avoid overwhelming users
- Test extra components thoroughly to ensure compatibility with Streamlit updates
- Follow the documentation for best practices on integrating extras into your app