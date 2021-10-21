package cs451.utils;

public class Generator {
    public static String[] range(int start, int end) {
        assert end > start;

        String[] values = new String[end-start];
        for (int i = 0; i < end-start; i++) {
            values[i] = Integer.toString(i+start);
        }

        return values;
    }
}
