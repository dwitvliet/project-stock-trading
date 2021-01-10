import os
import sys
import functools

import pandas as pd
import joblib
import tqdm

import utils

tqdm = functools.partial(tqdm.tqdm, file=sys.stdout, position=0, leave=True)


def fit_parameter_set(base_model, get_Xy, list_of_params, save_dir, skip_existing=True):

    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    Xy = None

    progress_bar = tqdm(list_of_params)
    progress_bar.set_description('Fitting')
    for iteration, params in enumerate(progress_bar):

        model_path = os.path.join(
            save_dir,
            f'{iteration}_{utils.utils.serialize_dict(params)}.pkl'
        )
        if skip_existing and os.path.exists(model_path):
            continue

        if Xy is None:
            Xy = get_Xy()

        model = base_model(**params)
        model.fit(*Xy)
        joblib.dump(model, model_path)


def score_models(model_dir, get_Xy_train, get_Xy_test, metrics):

    def get_model_idx(model_name):
        return int(model_name.replace('_', '.').split('.')[0])

    model_fnames = sorted(os.listdir(model_dir), key=get_model_idx)
    model_indices = [get_model_idx(fname) for fname in model_fnames]

    results = pd.DataFrame(
        index=model_indices,
        columns=[
            f'{train_or_test}_{metric_name}'
            for train_or_test in (['train', 'test'] if get_Xy_train else ['test'])
            for metric_name, _ in metrics
        ],
        dtype=float
    )

    Xy_train, Xy_test = None, None

    progress_bar = tqdm(model_fnames)
    progress_bar.set_description('Scoring')
    for model_fname in progress_bar:
        model_idx = get_model_idx(model_fname)

        if Xy_train is None and get_Xy_train:
            Xy_train = get_Xy_train()
        if Xy_test is None:
            Xy_test = get_Xy_test()

        model = joblib.load(os.path.join(model_dir, model_fname))

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
