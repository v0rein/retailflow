from unittest.mock import MagicMock, patch
import pytest

from ingestion.upload_to_adls import BRONZE_TABLES, upload_directory, upload_raw_file


def test_upload_directory_puts_each_file_in_its_table_folder(tmp_path, monkeypatch):
    source = tmp_path / "raw"
    source.mkdir()
    for file_name in BRONZE_TABLES:
        (source / file_name).write_text("id\n1\n", encoding="utf-8")
    (source / "notes.txt").write_text("not an Olist file", encoding="utf-8")
    mock_target = tmp_path / "lakehouse"
    monkeypatch.setenv("LOCAL_LAKEHOUSE_ROOT", str(mock_target))
    monkeypatch.setenv("USE_LOCAL_STORAGE", "true")

    results = upload_directory(str(source), layer="bronze")

    assert results == {file_name: True for file_name in BRONZE_TABLES}
    assert (mock_target / "bronze" / "orders" / "olist_orders_dataset.csv").exists()
    assert (mock_target / "bronze" / "payments" / "olist_order_payments_dataset.csv").exists()
    for file_name, table in BRONZE_TABLES.items():
        assert (mock_target / "bronze" / table / file_name).exists()
    # Files outside the mapping are not uploaded, so no flat CSVs land in bronze/.
    assert not list((mock_target / "bronze").glob("*.*"))


def test_upload_directory_missing_olist_file_raises(tmp_path, monkeypatch):
    source = tmp_path / "raw"
    source.mkdir()
    monkeypatch.setenv("LOCAL_LAKEHOUSE_ROOT", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("USE_LOCAL_STORAGE", "true")

    with pytest.raises(FileNotFoundError):
        upload_directory(str(source), layer="bronze")


def test_upload_raw_file_local_mock(tmp_path, monkeypatch):
    test_file = tmp_path / "sample.csv"
    test_file.write_text("order_id,price\n1,10.5\n", encoding="utf-8")
    mock_target = tmp_path / "lakehouse"
    monkeypatch.setenv("LOCAL_LAKEHOUSE_ROOT", str(mock_target))
    monkeypatch.setenv("USE_LOCAL_STORAGE", "true")

    success = upload_raw_file(str(test_file), layer="bronze", subpath="orders/sample.csv")
    assert success is True
    dest = mock_target / "bronze" / "orders" / "sample.csv"
    assert dest.exists()
    assert "order_id,price" in dest.read_text(encoding="utf-8")


def test_upload_raw_file_missing_file_raises(tmp_path):
    missing = tmp_path / "missing.csv"
    with pytest.raises(FileNotFoundError):
        upload_raw_file(str(missing), layer="bronze", subpath="orders/missing.csv")


def test_upload_raw_file_adls_mock(tmp_path, monkeypatch):
    test_file = tmp_path / "sample.csv"
    test_file.write_text("order_id,price\n1,10.5\n", encoding="utf-8")
    monkeypatch.setenv("USE_LOCAL_STORAGE", "false")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "AccountName=test;AccountKey=key;")
    monkeypatch.setenv("AZURE_CONTAINER_NAME", "retailflow")

    mock_blob_client = MagicMock()
    mock_service_client = MagicMock()
    mock_service_client.get_blob_client.return_value = mock_blob_client
    mock_blob_class = MagicMock()
    mock_blob_class.from_connection_string.return_value = mock_service_client
    mock_blob_module = MagicMock(BlobServiceClient=mock_blob_class)

    with patch.dict("sys.modules", {
        "azure": MagicMock(),
        "azure.storage": MagicMock(),
        "azure.storage.blob": mock_blob_module,
    }):
        success = upload_raw_file(str(test_file), layer="bronze", subpath="orders\\sample.csv")
        assert success is True
        mock_service_client.get_blob_client.assert_called_once_with(
            container="retailflow", blob="bronze/orders/sample.csv"
        )
        mock_blob_client.upload_blob.assert_called_once()


def test_upload_raw_file_adls_missing_conn_str_raises(tmp_path, monkeypatch):
    test_file = tmp_path / "sample.csv"
    test_file.write_text("order_id,price\n1,10.5\n", encoding="utf-8")
    monkeypatch.setenv("USE_LOCAL_STORAGE", "false")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)

    with pytest.raises(ValueError, match="Missing AZURE_STORAGE_CONNECTION_STRING"):
        upload_raw_file(str(test_file), layer="bronze", subpath="orders/sample.csv")
