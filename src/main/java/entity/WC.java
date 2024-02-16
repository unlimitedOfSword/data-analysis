package entity;

public class WC {

    private String wordName;
    private int freq;

    public WC() {
    }

    public WC(String wordName, int freq) {
        this.wordName = wordName;
        this.freq = freq;
    }

    public String getWordName() {
        return wordName;
    }

    public void setWordName(String wordName) {
        this.wordName = wordName;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }
}
