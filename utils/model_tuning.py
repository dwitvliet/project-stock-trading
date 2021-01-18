import os
import sys
import functools

import pandas as pd
import joblib
import tqdm

import utils

# Simplify `tqdm` calling.
tqdm = functools.partial(tqdm.tqdm, file=sys.stdout, position=0, leave=True)


def fit_multiple_parameters(base_model, get_Xy, list_of_params, save_dir, skip_existing=True):
    """ Fit multiple models using different parameters on the same data.

    Args:
        base_model (func): Function that takes the parameters as keyword
            arguments and returns a model to fit.
        get_Xy (func): Function that returns the training data as (X, y). This
            function is not called if `skip_existing` is `True` and all models
            are already generated.
        list_of_params (list of dicts): List with different parameter sets to
            fit models for.
        save_dir (str): Path to store fitted models.
        skip_existing (bool, optional): Whether to skip models already stored.

    """

    # Create save directory if it does not exist.
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    Xy = None

    progress_bar = tqdm(list_of_params)
    progress_bar.set_description('Fitting')
    for iteration, params in enumerate(progress_bar):

        # Skip existing model if requested.
        model_path = os.path.join(
            save_dir, f'{iteration}_{utils.utils.serialize(params)}.pkl'
        )
        if skip_existing and os.path.exists(model_path):
            continue

        # Fetch data and fit model
        if Xy is None:
            Xy = get_Xy()
        model = base_model(**params)
        model.fit(*Xy)

        # Store model.
        joblib.dump(model, model_path)


def fit_multiple_Xy(model, get_Xy, iterator, save_dir, skip_existing=True):
    """ Fit the same model multiple times using different data.

    Args:
        model (sklearn.model): Model to fit.
        get_Xy (func): Function that takes items of `iterator` as an argument
            and returns the training data as (X, y). This function is not called
            if `skip_existing` is `True` and the model is already generated.
        iterator (list): List to iterate over and use as arguments for `get_Xy`.
        save_dir (str): Path to store fitted models.
        skip_existing (bool, optional): Whether to skip models already stored

    Returns:

    """

    progress_bar = tqdm(iterator)
    progress_bar.set_description('Fitting')
    for iteration, item in enumerate(progress_bar):

        # Skip existing model if requested.
        model_path = os.path.join(
            save_dir, f'{iteration}_{utils.utils.serialize(item)}.pkl'
        )
        if skip_existing and os.path.exists(model_path):
            continue

        # Fit and store model.
        model.fit(*get_Xy(iteration))
        joblib.dump(model, model_path)


def score_models(model_dir, get_Xy_train, get_Xy_test, metrics, changing_Xy=False):
    """ Score multiple models using a set of metrics.

    Args:
        model_dir (str): Path to fitted models. Each model's filename should
            begin with the model index and an underscore.
        get_Xy_train (func): Function that returns the training data as (X, y).
            Should take the model index as an argument. Can be `None` if the
            metrics are not to be run on the training data.
        get_Xy_test (func): Function that returns the test data as (X, y).
            Should take the model index as an argument.
        metrics (list): The metrics to score the model on.
        changing_Xy (bool, optional): Whether the data changes for every model.
            If `False`, the data functions are called only once. If `True`, the
            functions are called for each model.

    Returns:
        pd.DataFrame

    """

    # Get list of all models to score.
    def get_model_idx(model_name):
        return int(model_name.split('_')[0])
    model_fnames = sorted(os.listdir(model_dir), key=get_model_idx)
    model_indices = [get_model_idx(fname) for fname in model_fnames]

    # Initialize dataframe to store results.
    results = pd.DataFrame(
        index=model_indices,
        columns=[
            f'{train_or_test}_{metric_name}'
            for train_or_test in (['train', 'test'] if get_Xy_train else ['test'])
            for metric_name, _ in metrics
        ],
        dtype=float
    )

    # Iterate all models.
    Xy_train, Xy_test = None, None
    progress_bar = tqdm(model_fnames)
    progress_bar.set_description('Scoring')
    for model_fname in progress_bar:
        model_idx = get_model_idx(model_fname)

        # Load data if first model or data changes on every model.
        if (Xy_train is None or changing_Xy) and get_Xy_train:
            Xy_train = get_Xy_train(model_idx)
        if Xy_test is None or changing_Xy:
            Xy_test = get_Xy_test(model_idx)

        # Load model.
        model = joblib.load(os.path.join(model_dir, model_fname))

        # Score metrics on test data and training data if requested.
        to_score = [('test', Xy_test)]
        if get_Xy_train:
            to_score.append(('train', Xy_train))
        for train_or_test, (X, y) in to_score:
            y_pred = model.predict(X)
            for metric_name, metric in metrics:
                results.loc[
                    model_idx, f'{train_or_test}_{metric_name}'
                ] = metric(y, y_pred)

    return results
