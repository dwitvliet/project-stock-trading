import os
import sys
import functools

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
