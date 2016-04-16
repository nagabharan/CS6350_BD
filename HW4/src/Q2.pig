/* Load dataset */
Movies = LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID,Title,Genres);

Ratings = LOAD '/Spring-2016-input/ratings.dat'  USING PigStorage(':') AS (UserID,MovieID,Rating,Timestamp);

/* Cogroup using MovieID */
MoviesRatings = COGROUP Movies BY MovieID, Ratings BY MovieID;

/* Get first 5 */
final = limit MoviesRatings 5;

dump final;