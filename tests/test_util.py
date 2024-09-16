"""
Tests for utility functions
"""

from pathlib import Path
import requests_mock

from dart_pipeline.util import download_file


def test_download_file():
    url = "http://example.com/a.csv"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "delete_this.csv"
    with requests_mock.Mocker() as m:
        m.get("http://example.com/a.csv", text="colA,colB\n1,2")
        download_file(url, path)
        assert path.read_text() == "colA,colB\n1,2"
        path.unlink()


def test_download_file_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path)
        # download_file should unpack zip files by default
        assert (source_path / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "world.txt").read_text() == "Hello world\n"
        path.unlink()
        (source_path / "hello.txt").unlink()
        (source_path / "world.txt").unlink()


def test_download_file_without_unzip():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=False)
        assert not (source_path / "hello.txt").exists()
        assert path.exists()
        path.unlink()


def test_download_file_unzip_create_folder():
    url = "http://example.com/file.zip"
    source_path = Path("tests/sources")
    source_path.mkdir(exist_ok=True)
    path = source_path / "file.zip"
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/file.zip",
            content=Path("tests/example.zip").read_bytes(),
        )
        download_file(url, path, unpack=True, unpack_create_folder=True)
        assert (source_path / "file" / "hello.txt").read_text() == "Hello there\n"
        assert (source_path / "file" / "world.txt").read_text() == "Hello world\n"
        (source_path / "file" / "hello.txt").unlink()
        (source_path / "file" / "world.txt").unlink()
