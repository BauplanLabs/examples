# A machine learning pipeline

In this example we show how to organize and run a simple machine learning project with bauplan. We will build and run a pipeline that takes some raw data from the [TLC NY taxi dataset](taxi), transforms it into a training dataset with the right features, trains a Linear Regression model to predict **the tip amount** of taxi rides, and writes the predictions to an Iceberg table. We will also use the Bauplan SDK in some notebooks to explore the dataset and the predictions.

## Preliminary steps

👉👉👉 To use Bauplan, you need an API key for our preview environment: you can request one [here](https://www.bauplanlabs.com/#join).

If you want to get familiar with Bauplan, start with our [tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/01_quick_start.html#)
