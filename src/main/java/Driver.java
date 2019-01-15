public class Driver {
    public static void main(String[] args) throws Exception {
        UserDivider userDivider = new UserDivider();
        MovieRelationFinder movieRelationFinder = new MovieRelationFinder();
        Normalizer normalizer = new Normalizer();
        Multiplicator multiplicator = new Multiplicator();
        MultiplicationSum multiplicationsum = new MultiplicationSum();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        String[] path1 = {rawInput, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
        String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
        String[] path4 = {normalizeDir, rawInput, multiplicationDir};
        String[] path5 = {multiplicationDir, sumDir};

        userDivider.main(path1);
        movieRelationFinder.main(path2);
        normalizer.main(path3);
        multiplicator.main(path4);
        multiplicationsum.main(path5);
    }
}
