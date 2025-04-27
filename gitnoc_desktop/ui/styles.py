"""
Application stylesheet for GitNOC Desktop based on JetBrains Darcula theme.
"""

STYLESHEET = """
QMainWindow {
    background-color: #2B2B2B;
    color: #A9B7C6;
}

QWidget {
    background-color: #2B2B2B;
    color: #A9B7C6;
    font-family: Menlo, Monaco, Consolas, monospace;
    font-size: 13px;
}

QLabel {
    color: #A9B7C6;
}

QTabWidget::pane {
    border: 1px solid #323232;
    background-color: #2B2B2B;
}

QTabBar::tab {
    background-color: #3C3F41;
    color: #A9B7C6;
    padding: 8px 15px;
    border: none;
    min-width: 80px;
}

QTabBar::tab:selected {
    background-color: #4E5254;
}

QTabBar::tab:hover {
    background-color: #45494A;
}

QListWidget {
    background-color: #2B2B2B;
    border: 1px solid #323232;
    outline: none;
}

QListWidget::item {
    padding: 5px;
    border-radius: 3px;
}

QListWidget::item:selected {
    background-color: #2D5B89;
    color: #FFFFFF;
}

QListWidget::item:hover {
    background-color: #3C3F41;
}

QPushButton {
    background-color: #365880;
    color: #FFFFFF;
    border: none;
    padding: 5px 15px;
    border-radius: 3px;
}

QPushButton:hover {
    background-color: #406C99;
}

QPushButton:pressed {
    background-color: #2D5B89;
}

QPushButton:disabled {
    background-color: #3C3F41;
    color: #808080;
}

QSplitter::handle {
    background-color: #323232;
}

QSplitter::handle:horizontal {
    width: 2px;
}

QSplitter::handle:vertical {
    height: 2px;
}

QScrollBar:vertical {
    border: none;
    background-color: #2B2B2B;
    width: 12px;
    margin: 0px;
}

QScrollBar::handle:vertical {
    background-color: #404040;
    min-height: 20px;
    border-radius: 6px;
}

QScrollBar::handle:vertical:hover {
    background-color: #4D4D4D;
}

QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical {
    height: 0px;
}

QScrollBar:horizontal {
    border: none;
    background-color: #2B2B2B;
    height: 12px;
    margin: 0px;
}

QScrollBar::handle:horizontal {
    background-color: #404040;
    min-width: 20px;
    border-radius: 6px;
}

QScrollBar::handle:horizontal:hover {
    background-color: #4D4D4D;
}

QScrollBar::add-line:horizontal,
QScrollBar::sub-line:horizontal {
    width: 0px;
}

QMessageBox {
    background-color: #2B2B2B;
}

QMessageBox QLabel {
    color: #A9B7C6;
}

QInputDialog {
    background-color: #2B2B2B;
}

QInputDialog QLabel {
    color: #A9B7C6;
}

QLineEdit {
    background-color: #45494A;
    color: #A9B7C6;
    border: 1px solid #323232;
    padding: 5px;
    border-radius: 3px;
}

QLineEdit:focus {
    border: 1px solid #365880;
}

/* Table and Tree Views */
QTableView {
    background-color: #2B2B2B;
    alternate-background-color: #313335;
    gridline-color: #323232;
    border: 1px solid #323232;
    selection-background-color: #2D5B89;
    selection-color: #FFFFFF;
}

QHeaderView::section {
    background-color: #3C3F41;
    color: #A9B7C6;
    padding: 5px;
    border: none;
}

QTreeView {
    background-color: #2B2B2B;
    alternate-background-color: #313335;
    border: 1px solid #323232;
}

QTreeView::item:selected {
    background-color: #2D5B89;
    color: #FFFFFF;
}

QTreeView::item:hover {
    background-color: #3C3F41;
}

QToolTip {
    background-color: #3C3F41;
    color: #A9B7C6;
    border: 1px solid #323232;
    padding: 5px;
}
""" 