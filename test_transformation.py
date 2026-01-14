import pandas as pd
from src.transformation.spark_transformer import SparkTransformer

def test_spark_transformation():
    df1 = pd.DataFrame({
        "title": ["Book A"],
        "price": [10.0],
        "source": ["csv"]
    })

    df2 = pd.DataFrame({
        "title": ["Book B"],
        "price": [12.5],
        "source": ["web"]
    })

    transformer = SparkTransformer()
    df_result = transformer.transform([df1, df2])

    assert df_result.count() == 2
