def test_csv_extraction():
    from src.extraction.csv_extractor import CSVExtractor

    extractor = CSVExtractor(
        csv_url="https://drive.google.com/uc?id=1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD"
    )
    df = extractor.extract()
    assert not df.empty
