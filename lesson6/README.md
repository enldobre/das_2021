**Requirements:**
numpy --- add package to requirements.txt  
pandas --- add package to requirements.txt  
matplotlib --- add package to requirements.txt  
**Environment:**

# Exercises & examples:
Spark Machine Learning Exercises:
1. Prepare datasets
2. Collaborative Filtering
3. Random Forest Classifier



**Collaborative filtering**

In Collaborative filtering we make predictions (filtering) about the interests of a user by collecting preferences or taste information from many users (collaborating).  
The underlying assumption is that if a user A has the same opinion as a user B on an issue,  
A is more likely to have B's opinion on a different issue x than to have the opinion on x of a user chosen randomly.

Spark MLlib library for Machine Learning provides a Collaborative Filtering implementation by using Alternating Least Squares.  
The implementation in MLlib has the following parameters:

- numBlocks is the number of blocks used to parallelize computation (set to -1 to auto-configure).
- rank is the number of latent factors in the model.
- iterations is the number of iterations to run.
- lambda specifies the regularization parameter in ALS.
- implicitPrefs specifies whether to use the explicit feedback ALS variant or one adapted for implicit feedback data.
- alpha is a parameter applicable to the implicit feedback variant of ALS that governs the baseline confidence in preference observations.

**Random Forest Classifier**

The scenario involves analyzing titanic data in order to predict for survivors.  
This is actually an estimator that we have to fit after we prepare the data.



