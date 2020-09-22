import autosklearn.classification
import sklearn.model_selection
import sklearn.datasets
import sklearn.metrics

X, y = sklearn.datasets.load_digits(return_X_y=True)
print('loaded')
X_train, X_test, y_train, y_test = \
        sklearn.model_selection.train_test_split(X, y, random_state=1)
print('splitted')
automl = autosklearn.classification.AutoSklearnClassifier(include_estimators=['liblinear_svc'])
automl.fit(X_train, y_train)
print('fited')
y_hat = automl.predict(X_test)
print("Accuracy score", sklearn.metrics.accuracy_score(y_test, y_hat))