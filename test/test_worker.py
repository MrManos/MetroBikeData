from worker import calculate_similarities

def test_calculate_similarities():
    result = calculate_similarities("HGNC:24523", "HGNC:29027")
    assert isinstance(result, str)