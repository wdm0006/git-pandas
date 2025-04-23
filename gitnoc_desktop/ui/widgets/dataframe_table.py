from PySide6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PySide6.QtCore import Qt
import pandas as pd

class DataFrameTable(QTableWidget):
    """A QTableWidget subclass specifically designed for displaying pandas DataFrames.
    
    Features:
    - Automatic sorting
    - Proper handling of different data types
    - Index display
    - Consistent formatting
    - Automatic column sizing
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Enable sorting
        self.setSortingEnabled(True)
        
        # Set alternating row colors for better readability
        self.setAlternatingRowColors(True)
        
        # Disable editing
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        
    def set_dataframe(self, df, columns=None, show_index=True, stretch_last=True, max_visible_rows=20):
        """Set the DataFrame to display.
        
        Args:
            df (pd.DataFrame): DataFrame to display
            columns (list, optional): List of column names to show. If None, shows all columns.
            show_index (bool): Whether to show the DataFrame's index as a column
            stretch_last (bool): Whether to stretch the last column
            max_visible_rows (int): Maximum number of rows to show without scrolling
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Data must be a pandas DataFrame")
            
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Reset index if showing it
        if show_index:
            if isinstance(df.index, pd.MultiIndex):
                # For MultiIndex, concatenate the levels
                index_values = df.index.map(lambda x: ' / '.join(str(i) for i in x))
                df = df.reset_index(drop=True)
                df.insert(0, 'Index', index_values)
            else:
                # For regular Index
                df = df.reset_index(drop=False)
                df = df.rename(columns={df.index.name or 'index': 'Index'})
        
        # Determine columns to show
        if columns is None:
            columns = list(df.columns)
        else:
            # Ensure all requested columns exist
            missing = [col for col in columns if col not in df.columns]
            if missing:
                raise ValueError(f"Columns not found in DataFrame: {missing}")
        
        # Set up the table
        self.setRowCount(len(df))
        self.setColumnCount(len(columns))
        
        # Set headers
        headers = []
        for col in columns:
            header = col.replace('_', ' ').title()
            headers.append(header)
        self.setHorizontalHeaderLabels(headers)
        
        # Populate data
        for i in range(len(df)):
            for j, col in enumerate(columns):
                value = df.iloc[i][col]
                
                # Handle different data types
                if pd.isna(value):
                    display_value = ''
                elif isinstance(value, bool):
                    display_value = '✓' if value else '✗'
                elif isinstance(value, (int, float)):
                    if isinstance(value, float):
                        display_value = f"{value:.2f}"  # Format floats to 2 decimal places
                    else:
                        display_value = str(value)
                elif isinstance(value, pd.Timestamp):
                    display_value = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    display_value = str(value)
                
                item = QTableWidgetItem(display_value)
                
                # Set sort value
                if isinstance(value, (int, float)) and pd.notna(value):
                    item.setData(Qt.ItemDataRole.EditRole, float(value))
                
                # Right-align numeric values
                if isinstance(value, (int, float)):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                
                self.setItem(i, j, item)
        
        # Adjust column widths
        self.resizeColumnsToContents()
        
        # Set reasonable minimum width for the table
        total_width = sum(self.columnWidth(i) for i in range(self.columnCount()))
        self.setMinimumWidth(min(total_width + 50, 1200))  # Add padding, cap at 1200px
        
        # Set reasonable height
        header_height = self.horizontalHeader().height()
        row_height = self.rowHeight(0) if self.rowCount() > 0 else 30
        visible_rows = min(max_visible_rows, self.rowCount())
        table_height = header_height + (row_height * visible_rows) + 2  # +2 for borders
        self.setMinimumHeight(table_height)
        
        # Configure column resizing
        header = self.horizontalHeader()
        if stretch_last:
            for j in range(len(columns) - 1):
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.Interactive)
            header.setSectionResizeMode(len(columns) - 1, QHeaderView.ResizeMode.Stretch)
        else:
            for j in range(len(columns)):
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.Interactive) 