/* Load dataset */
Movies = load '/home/nagabharan/Desktop/HW4/dataset/movies.dat' using PigStorage('#') as (MovieID:chararray,Title:chararray, Genres:chararray);

Ratings = load '/home/nagabharan/Desktop/HW4/dataset/ratings.dat' using PigStorage('#') as (UserID:chararray,MovieID:chararray, Rating:double, Timestamp:int);

Users = load '/home/nagabharan/Desktop/HW4/dataset/users.dat' using PigStorage('#') as (UserID:chararray,Gender:chararray, Age:int, Occupation:chararray, Zipcode:chararray);

/* Join Movie and Ratings */
MovieRatings = join Movies by(MovieID), Ratings by(MovieID);

/* Filter for Comedy and Drama Movies */
ComedyDrama = filter MovieRatings by (Genres matches '.*Comedy.*' and Genres matches '.*Drama.*');

/* Group by MovieID and and Order by ratings to get lowest rating */
groupA = group ComedyDrama by $0;
groupB = foreach groupA generate flatten(group), AVG(ComedyDrama.$5);
groupdesc = order groupB by $1 desc;

Limitdesc = limit groupdesc 1;
tmp= foreach Limitdesc generate $1;

mvs= join groupB by ($1), tmp by ($0);
rat = join Ratings by(MovieID), mvs by($0);
usrrat = join rat by($0), Users by($0);

/* Apply filter for Male aged 20-40 in zipcode 7. */
Age = filter usrrat by (Gender matches '.*M.*' and (Age > 20 AND Age < 40) and Zipcode matches '7.*');
final_userID = foreach Age generate $0;

final = distinct final_userID;

dump final;