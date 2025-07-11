# Excel Merging Tool User Guide

## Overview
The Excel Merging Tool (page 15_📊_Объединение_Excel.py) allows users to combine multiple Excel files into a single consolidated file while preserving the structure and applying various filters.

## Key Features

### 1. Template-Based Merging
- Upload a main template file that serves as the base structure
- All sheets from the template are preserved in the final result
- Selected sheets are merged with data from additional files
- Unselected sheets remain in their original form from the template

### 2. Selective Sheet Processing
- **Merged Sheets**: Selected sheets combine data from template + additional files
- **Preserved Sheets**: Unselected sheets are kept exactly as they appear in the template
- **No Data Loss**: All sheets from the template appear in the final file

### 3. Brand Filtering
- Automatic detection of "Бренд в одежде и обуви*" column
- Filter data by specific brand across all files
- Maintains headers while filtering data rows
- Configurable and optional

### 4. Article-Based Filtering
- Special filtering for "Озон.Видео" and "Озон.Видеообложка" sheets
- Uses articles from template "Шаблон" sheet to filter video content
- Ensures consistency across video-related data

### 5. Progress Tracking
- Real-time progress updates during processing
- Detailed status messages for each operation
- Error handling with specific error messages

## How to Use

### Step 1: Upload Template File
1. Click "Выберите файл-шаблон" 
2. Select your main Excel file (.xlsx format)
3. The system will display all available sheets

### Step 2: Upload Additional Files
1. Click "Выберите дополнительные файлы"
2. Select multiple Excel files to merge with the template
3. All files must have the same sheet structure as template

### Step 3: Configure Brand Filtering (Optional)
1. If "Бренд в одежде и обуви*" column is detected, you can enable filtering
2. Enter the brand name to filter by
3. Check "Применить фильтр" to enable
4. Only rows matching the brand will be included

### Step 4: Configure Sheet Merging
1. For each sheet, decide whether to merge or preserve:
   - **Checked "Объединить"**: Sheet will be merged with data from additional files
   - **Unchecked**: Sheet will be preserved in original form from template
2. Set starting row for data extraction from additional files (usually row 5)

### Step 5: Execute Merging
1. Review the summary showing which sheets will be merged vs preserved
2. Click "🔗 Объединить файлы"
3. Monitor progress and wait for completion
4. Download the resulting merged file

## Important Notes

### Sheet Preservation
- **All sheets from the template are included in the final file**
- Sheets not selected for merging remain exactly as they were in the template
- This ensures no data is lost from the original template structure

### Data Processing Order
1. Template sheets are loaded first
2. Brand filtering is applied if enabled
3. Additional files are processed and filtered
4. Data is merged for selected sheets only
5. Unselected sheets remain untouched

### Performance Considerations
- Large files may take several minutes to process
- Progress bar shows real-time status
- Don't close the browser during processing
- Memory usage depends on file sizes

### Error Handling
- Individual file errors don't stop the entire process
- Missing sheets in additional files are skipped
- Detailed error messages help identify issues
- Backup recommendations for important data

## Troubleshooting

### Common Issues
1. **"Sheet not found" errors**: Additional files missing sheets that exist in template
2. **Memory errors**: Files too large, try reducing file sizes
3. **Brand column not detected**: Check column name format in template
4. **Slow processing**: Normal for large files, wait for completion

### Best Practices
1. Ensure all files have consistent sheet names and structure
2. Test with small files first
3. Use brand filtering to reduce data volume when possible
4. Keep backup copies of original files
5. Check the preview before processing large batches

## Technical Details

### Supported Formats
- Input: Excel (.xlsx) files only
- Output: Excel (.xlsx) format
- Encoding: UTF-8 with proper Excel formatting

### Processing Logic
- Template-first approach ensures structure preservation
- Pandas DataFrame operations for efficient data handling
- OpenPyXL for Excel file manipulation
- Streaming progress updates for user feedback

### Memory Management
- Temporary files for large data processing
- Automatic cleanup of temporary resources
- Optimized for cloud server environments