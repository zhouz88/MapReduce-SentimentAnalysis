public class Driver {
    public static void main(String[] args) throws Exception{
        SentimentAnalysis sentimentanalysis = new SentimentAnalysis();
        JSONConverter jsonConverter = new JSONConverter();

        String input = args[0];
        String output = args[1];
        String dictionary = args[2];
        String JSONoutput = args[3];

        String[] args0 = {input, output, dictionary};
        String[] args1 = {output, JSONoutput};
        sentimentanalysis.main(args0);
        jsonConverter.main(args1);
    }
}
