/* Load dataset */
Movies = load '/home/nagabharan/Desktop/HW4/dataset/movies.dat' using PigStorage('#') as (MovieID:chararray,Title:chararray, Genres:chararray);

Ratings = load '/home/nagabharan/Desktop/HW4/dataset/ratings.dat' using PigStorage('#') as (UserID:chararray,MovieID:chararray, Rating:double, Timestamp:int);

/* Cogroup using MovieID */
MoviesRatings = cogroup Movies by (MovieID), Ratings by (MovieID);

/* Get first 5 */
final = limit MoviesRatings 5;

dump final;