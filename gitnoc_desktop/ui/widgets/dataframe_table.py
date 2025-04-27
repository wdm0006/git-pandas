from PySide6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PySide6.QtCore import Qt
import pandas as pd

class DataFrameTable(QTableWidget):
    """
    QTableWidget for displaying pandas DataFrames with automatic formatting.
    
    Features: automatic sorting, data type handling, index display,
    consistent formatting, and column sizing.
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.setSortingEnabled(True)
        self.setAlternatingRowColors(True)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        
    def set_dataframe(self, df, columns=None, show_index=True, stretch_last=True, max_visible_rows=20):
        """
        Set and display a DataFrame in the table.
        
        Args:
            df: DataFrame to display
            columns: Column names to show (all columns if None)
            show_index: Whether to display DataFrame index as a column
            stretch_last: Whether to stretch the last column to fill space
            max_visible_rows: Maximum rows to show without scrolling
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Data must be a pandas DataFrame")
            
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Handle index display
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
        
        # Determine columns to display
        if columns is None:
            columns = list(df.columns)
        else:
            # Verify requested columns exist
            missing = [col for col in columns if col not in df.columns]
            if missing:
                raise ValueError(f"Columns not found in DataFrame: {missing}")
        
        # Configure table dimensions
        self.setRowCount(len(df))
        self.setColumnCount(len(columns))
        
        # Set column headers
        headers = [col.replace('_', ' ').title() for col in columns]
        self.setHorizontalHeaderLabels(headers)
        
        # Populate data cells
        for i in range(len(df)):
            for j, col in enumerate(columns):
                value = df.iloc[i][col]
                
                # Format based on data type
                if pd.isna(value):
                    display_value = ''
                elif isinstance(value, bool):
                    display_value = '✓' if value else '✗'
                elif isinstance(value, (int, float)):
                    if isinstance(value, float):
                        display_value = f"{value:.2f}"
                    else:
                        display_value = str(value)
                elif isinstance(value, pd.Timestamp):
                    display_value = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    display_value = str(value)
                
                item = QTableWidgetItem(display_value)
                
                # Set sort value for numeric data
                if isinstance(value, (int, float)) and pd.notna(value):
                    item.setData(Qt.ItemDataRole.EditRole, float(value))
                
                # Right-align numeric values
                if isinstance(value, (int, float)):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                
                self.setItem(i, j, item)
        
        # Size columns appropriately
        self.resizeColumnsToContents()
        
        # Set reasonable minimum width
        total_width = sum(self.columnWidth(i) for i in range(self.columnCount()))
        self.setMinimumWidth(min(total_width + 50, 1200))
        
        # Set reasonable height based on visible rows
        header_height = self.horizontalHeader().height()
        row_height = self.rowHeight(0) if self.rowCount() > 0 else 30
        visible_rows = min(max_visible_rows, self.rowCount())
        table_height = header_height + (row_height * visible_rows) + 2
        self.setMinimumHeight(table_height)
        
        # Configure column resize behavior
        header = self.horizontalHeader()
        if stretch_last and len(columns) > 0:
            for j in range(len(columns) - 1):
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.Interactive)
            header.setSectionResizeMode(len(columns) - 1, QHeaderView.ResizeMode.Stretch)
        else:
            for j in range(len(columns)):
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.Interactive) 