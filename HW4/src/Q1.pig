/* Load dataset */
movies =LOAD '/Spring-2016-input/movies.dat' USING PigStorage(':') AS (MovieID,Title,Genres);
ratings = LOAD '/Spring-2016-input/ratings.dat'  USING PigStorage(':') AS (UserID,MovieID,Rating,Timestamp);
users = LOAD '/Spring-2016-input/users.dat' USING PigStorage(':') AS (UserID,Gender,Age,Occupation,ZipCode);

/* Filter the data according to the parameters */
age20and35 = FILTER users BY (Age < 35) and (Age > 20);
female = FILTER age20and35 BY Gender == 'F';
zipfilter = FILTER male BY ZipCode matches '1.*';

/* Filter for Action and War Movies */
ActionWar = FILTER movies BY Genres == 'Action|War';

/* Join Movie and Ratings */
moviesJoinRating = JOIN ActionWar BY MovieID, ratings BY MovieID;
Afterjoin = foreach moviesJoinRating generate UserID,ratings::MovieID,Rating,Title,Genres;

/* Group by MovieID and and Order by ratings to get lowest rating */
groupByrating = GROUP Afterjoin BY MovieID;
averageRating = FOREACH groupByrating GENERATE group AS MovieID, AVG(Afterjoin.Rating) AS avgRating;
temp = GROUP averageRating ALL;
temp1 = FOREACH temp GENERATE FLATTEN(averageRating.(MovieID,avgRating)),MIN(averageRating.$1) AS min;
lowestRatedMovie = FILTER temp1 BY avgRating==min;

/* Get the movie with lowest rating and join the lowestratedmovie and filtereduser, then we get the desire user*/
lowestjoinuser = JOIN ratings BY MovieID, lowestRatedMovie BY MovieID;
form = FOREACH lowestjoinuser GENERATE UserID,ratings::MovieID;

result = JOIN form BY UserID, zipfilter BY UserID;
final = FOREACH result GENERATE form::ratings::UserID;

dump final;
