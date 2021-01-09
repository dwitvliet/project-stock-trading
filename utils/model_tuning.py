import os
import sys
import functools

import pandas as pd
import joblib
import tqdm

import utils

tqdm = functools.partial(tqdm.tqdm, file=sys.stdout, position=0, leave=True)


def fit_models(base_model, get_Xy, params, save_dir, skip_existing=True):

    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    Xy = None

    for iteration, param in enumerate(tqdm(params)):

        model_path = os.path.join(
            save_dir,
            f'{iteration}_{utils.utils.serialize_dict(param)}.pkl'
        )
        if skip_existing and os.path.exists(model_path):
            continue

        if Xy is None:
            Xy = get_Xy()

        model = base_model(**param)
        model.fit(*Xy)
        joblib.dump(model, model_path)


def score_models(model_dir, get_Xy_train, get_Xy_test, params, metrics):

    results = pd.DataFrame(
        index=range(100),
        columns=[
            f'{train_or_test}_{metric_name}'
            for train_or_test in ('train', 'test')
            for metric_name, _ in metrics
        ],
        dtype=float
    )

    Xy_train, Xy_test = None, None

    for iteration, param in enumerate(tqdm(params)):

        model_path = os.path.join(
            model_dir,
            f'{iteration}_{utils.utils.serialize_dict(param)}.pkl'
        )
        assert os.path.exists(model_path), f'Cannot find model {model_path}'

        if Xy_train is None or Xy_test is None:
            Xy_train = get_Xy_train()
            Xy_test = get_Xy_test()

        model = joblib.load(model_path)

        for train_or_test, (X, y) in [('train', Xy_train), ('test', Xy_test)]:
            y_pred = model.predict(X)
            for metric_name, metric in metrics:
                results.loc[
                    iteration, f'{train_or_test}_{metric_name}'
                ] = metric(y, y_pred)

    return results
