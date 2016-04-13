/* Load UDF jar */
REGISTER /home/nagabharan/Desktop/HW4/FORMAT_GENRE/pigudf1.jar;

/* Load dataset */
movies = LOAD '/home/nagabharan/Desktop/HW4/dataset/movies.dat' USING PigStorage(':') AS (MovieID:int,space:chararray,Title:chararray,space2:chararray,Genre:chararray);

/* Filter out null entries in genre and process the tuple in UDF */
tmp = FOREACH movies generate MovieID,Title,Genre;
tmp2 = FILTER tmp by Genre!='';
format_movies = FOREACH tmp2 GENERATE Title , CONCAT(FORMAT_GENRE(Genre) , 'nxn141730');

DUMP format_movies;
