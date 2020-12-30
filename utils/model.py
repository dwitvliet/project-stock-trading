from sklearn import metrics


def evaluate(model, train, test):
    train_X, train_y = train.iloc[:, 1:], train.iloc[:, 0]
    test_X, test_y = test.iloc[:, 1:], test.iloc[:, 0]

    model.fit(train_X, train_y)

    train_pred = model.predict(train_X)
    test_pred = model.predict(test_X)

    return f'''
    Train scores:
    Accuracy: {metrics.accuracy_score(train_y, train_pred)}
    Precision: {metrics.precision_score(train_y, train_pred, average='weighted')}
    Recall: {metrics.recall_score(train_y, train_pred, average='weighted')}
    F1: {metrics.f1_score(train_y, train_pred, average='weighted')}
    
    Test scores:
    Accuracy: {metrics.accuracy_score(test_y, test_pred)}
    Precision: {metrics.precision_score(test_y, test_pred, average='weighted')}
    Recall: {metrics.recall_score(test_y, test_pred, average='weighted')}
    F1: {metrics.f1_score(test_y, test_pred, average='weighted')}
    '''
