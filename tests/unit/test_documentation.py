"""
Documentation validation tests.

This module tests the documentation structure, links, and content
to ensure everything is properly organized and functional.
"""

from pathlib import Path


def _find_project_root() -> Path:
    """Find the project root directory by looking for pyproject.toml."""
    current_path = Path(__file__).parent

    # Walk up the directory tree looking for pyproject.toml
    for parent in current_path.parents:
        if (parent / "pyproject.toml").exists():
            return parent

    # Fallback to assuming we're in tests/unit/ and project root is two levels up
    return Path(__file__).parent.parent.parent


class TestDocumentationStructure:
    """Test documentation file structure and organization."""

    def test_readme_exists_and_basic_structure(self):
        """Test that README.md exists and has basic structure."""
        project_root = _find_project_root()
        readme_path = project_root / "README.md"
        assert readme_path.exists(), "README.md should exist"

        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for essential sections
        assert "# Splurge Data Profiler" in content, "Should have main title"
        assert "## Features" in content, "Should have features section"
        assert "## Installation" in content, "Should have installation section"
        assert "## Quick Start" in content, "Should have quick start section"
        assert "## License" in content, "Should have license section"

    def test_changelog_exists_and_format(self):
        """Test that CHANGELOG.md exists and follows proper format."""
        project_root = _find_project_root()
        changelog_path = project_root / "CHANGELOG.md"
        assert changelog_path.exists(), "CHANGELOG.md should exist"

        with open(changelog_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for proper changelog format
        assert "# Changelog" in content, "Should have changelog title"
        assert "## [" in content, "Should have version sections"
        assert "All notable changes" in content, "Should have standard header"

    def test_detailed_docs_exists_and_structure(self):
        """Test that detailed documentation exists and has proper structure."""
        project_root = _find_project_root()
        docs_path = project_root / "docs" / "README-details.md"
        assert docs_path.exists(), "docs/README-details.md should exist"

        with open(docs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for detailed documentation structure
        assert "# Splurge Data Profiler - Detailed Documentation" in content
        assert "## Table of Contents" in content
        assert "## Features" in content
        assert "## CLI Usage" in content
        assert "## Configuration" in content

    def test_docs_directory_structure(self):
        """Test that docs directory has proper structure."""
        project_root = _find_project_root()
        docs_dir = project_root / "docs"
        assert docs_dir.exists(), "docs directory should exist"
        assert docs_dir.is_dir(), "docs should be a directory"

        # Check for required files
        required_files = ["README-details.md"]
        for filename in required_files:
            file_path = docs_dir / filename
            assert file_path.exists(), f"{filename} should exist in docs directory"


class TestDocumentationLinks:
    """Test documentation links and references."""

    def test_readme_links_to_detailed_docs(self):
        """Test that README.md properly links to detailed documentation."""
        project_root = _find_project_root()
        readme_path = project_root / "README.md"

        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for links to detailed documentation
        assert "docs/README-details.md" in content, "README should link to detailed docs"
        assert "CHANGELOG.md" in content, "README should link to changelog"

    def test_detailed_docs_internal_links(self):
        """Test that detailed documentation has proper internal links."""
        project_root = _find_project_root()
        docs_path = project_root / "docs" / "README-details.md"

        with open(docs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for internal section links
        assert "[Features](#features)" in content, "Should have features link"
        assert "[CLI Usage](#cli-usage)" in content, "Should have CLI usage link"
        assert "[Configuration](#configuration)" in content, "Should have configuration link"

    def test_pyproject_urls_point_to_correct_locations(self):
        """Test that pyproject.toml URLs point to correct documentation locations."""
        project_root = _find_project_root()
        pyproject_path = project_root / "pyproject.toml"

        with open(pyproject_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for proper URL configurations
        assert "README-details.md" in content, "Should reference detailed docs"
        assert "CHANGELOG.md" in content, "Should reference changelog"
        assert "github.com/jim-schilling/splurge-data-profiler" in content, "Should have GitHub URLs"


class TestDocumentationContent:
    """Test documentation content quality and completeness."""

    def test_readme_has_badges(self):
        """Test that README.md has proper badges."""
        project_root = _find_project_root()
        readme_path = project_root / "README.md"

        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for PyPI badges
        assert "pypi.org/project/splurge-data-profiler" in content, "Should have PyPI links"
        assert "img.shields.io" in content, "Should have shield badges"

    def test_detailed_docs_comprehensive(self):
        """Test that detailed documentation is comprehensive."""
        project_root = _find_project_root()
        docs_path = project_root / "docs" / "README-details.md"

        with open(docs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for comprehensive content
        required_sections = [
            "Configuration",
            "Examples",
            "Data Types",
            "Adaptive Sampling",
            "Programmatic Usage",
            "Requirements",
            "Error Handling",
            "API Reference",
        ]

        for section in required_sections:
            assert f"## {section}" in content, f"Should have {section} section"

    def test_changelog_has_versions(self):
        """Test that changelog contains version information."""
        project_root = _find_project_root()
        changelog_path = project_root / "CHANGELOG.md"

        with open(changelog_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for version entries
        assert "[0.1.1]" in content, "Should have version 0.1.1"
        assert "[0.1.0]" in content, "Should have version 0.1.0"

    def test_documentation_formatting(self):
        """Test that documentation follows proper formatting."""
        project_root = _find_project_root()
        docs_path = project_root / "docs" / "README-details.md"

        with open(docs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for proper markdown formatting
        assert "```" in content, "Should have code blocks"
        assert "- " in content, "Should have bullet points"
        assert "##" in content, "Should have headers"

    def test_examples_are_executable(self):
        """Test that examples in documentation are properly formatted."""
        project_root = _find_project_root()
        docs_path = project_root / "docs" / "README-details.md"

        with open(docs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for properly formatted examples
        assert "python -m splurge_data_profiler" in content, "Should have CLI examples"
        assert "```python" in content, "Should have Python code blocks"
        assert "```bash" in content, "Should have bash code blocks"


class TestDocumentationConsistency:
    """Test consistency across documentation files."""

    def test_feature_descriptions_consistent(self):
        """Test that feature descriptions are consistent across docs."""
        project_root = _find_project_root()
        readme_path = project_root / "README.md"
        docs_path = project_root / "docs" / "README-details.md"

        with open(readme_path, "r", encoding="utf-8") as f:
            readme_content = f.read()

        with open(docs_path, "r", encoding="utf-8") as f:
            docs_content = f.read()

        # Check for consistent feature mentions
        features = ["DSV File Support", "Automatic Type Inference", "Data Lake Creation"]
        for feature in features:
            assert feature in readme_content, f"README should mention {feature}"
            assert feature in docs_content, f"Detailed docs should mention {feature}"

    def test_no_duplicate_content(self):
        """Test that there's no significant duplicate content between docs."""
        project_root = _find_project_root()
        readme_path = project_root / "README.md"
        docs_path = project_root / "docs" / "README-details.md"

        with open(readme_path, "r", encoding="utf-8") as f:
            readme_content = f.read()

        with open(docs_path, "r", encoding="utf-8") as f:
            docs_content = f.read()

        # README should be concise, detailed docs should be comprehensive
        assert len(readme_content) < len(docs_content), "README should be shorter than detailed docs"

        # Check that detailed examples are only in detailed docs
        assert "Example 1:" in docs_content, "Detailed examples should be in detailed docs"
        assert "Example 1:" not in readme_content, "Detailed examples should not be in README"


def test_documentation_file_sizes():
    """Test that documentation files have reasonable sizes."""
    project_root = _find_project_root()

    # README should be concise
    readme_path = project_root / "README.md"
    with open(readme_path, "r", encoding="utf-8") as f:
        readme_size = len(f.read())
    assert readme_size < 5000, "README should be concise (< 5000 chars)"

    # Detailed docs should be comprehensive
    docs_path = project_root / "docs" / "README-details.md"
    with open(docs_path, "r", encoding="utf-8") as f:
        docs_size = len(f.read())
    assert docs_size > 10000, "Detailed docs should be comprehensive (> 10000 chars)"

    # Changelog should have reasonable size
    changelog_path = project_root / "CHANGELOG.md"
    with open(changelog_path, "r", encoding="utf-8") as f:
        changelog_size = len(f.read())
    assert changelog_size > 1000, "Changelog should have content (> 1000 chars)"
