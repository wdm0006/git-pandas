# --- Helper function for language mapping (basic) --- #
def get_language_from_extension(extension):
    """
    Map file extension to programming language name.
    
    Args:
        extension: File extension including the dot (e.g., '.py')
        
    Returns:
        String representing the programming language name
    """
    mapping = {
        ".py": "Python",
        ".js": "JavaScript",
        ".ts": "TypeScript",
        ".java": "Java",
        ".cs": "C#",
        ".cpp": "C++",
        ".c": "C",
        ".h": "C/C++ Header",
        ".rb": "Ruby",
        ".php": "PHP",
        ".html": "HTML",
        ".css": "CSS",
        ".md": "Markdown",
        ".json": "JSON",
        ".yaml": "YAML",
        ".yml": "YAML",
        ".xml": "XML",
        ".sh": "Shell",
        ".go": "Go",
        ".rs": "Rust",
        ".swift": "Swift",
        ".kt": "Kotlin",
        ".scala": "Scala",
        # Add more as needed
    }
    return mapping.get(extension.lower(), f"Other ({extension})") 