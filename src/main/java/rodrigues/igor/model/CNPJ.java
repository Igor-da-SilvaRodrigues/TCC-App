package rodrigues.igor.model;

import java.util.Arrays;
import java.util.Random;

public class CNPJ {
    private int[] cnpj;

    public String getAsString(){
        StringBuilder builder = new StringBuilder();
        for (int i : cnpj){
            builder.append(i);
        }
        return builder.toString();
    }

    /**
     * Returns the CNPJ represented by the string. Or null if the string is blank;
     */
    public static CNPJ fromString(String s){
        if(s == null || s.isBlank()){
            return null;
        }

        CNPJ cnpj1 = new CNPJ();
        cnpj1.setString(s);
        return cnpj1;
    }

    public void setString(String s){
        if (s.length() != 14){
            throw new IllegalArgumentException("Wrong string size: " + s.length());
        }

        for(int i = 0; i < s.length(); i++){
            cnpj[i] = Character.getNumericValue(s.charAt(i));
        }
    }

    public CNPJ() {
        cnpj = new int[14];
        Random random = new Random();

        //generate first 12 numbers
        for (int i = 0; i < 12; i++){
            cnpj[i] = random.nextInt(10);
        }

        int first = calculateFirstDigit(cnpj);
        cnpj[12] = first;

        int second = calculateSecondDigit(cnpj);
        cnpj[13] = second;
    }

    private int calculateSecondDigit(int[] cnpj) {
        int[] multipliers = {6,5,4,3,2,9,8,7,6,5,4,3,2};
        if(cnpj.length < 13){
            throw new RuntimeException("Not enough digits to calculate");
        }

        int sum = 0;
        for(int i = 0; i < 13; i++){
            sum += cnpj[i] * multipliers[i];
        }

        int mod = sum % 11;

        return mod < 2 ? 0 : 11-mod;
    }

    private int calculateFirstDigit(int[] cnpj) {
        int[] multipliers = {5,4,3,2,9,8,7,6,5,4,3,2};
        if(cnpj.length < 12){
            throw new RuntimeException("Not enough digits to calculate");
        }

        int sum = 0;
        for(int i = 0; i < 12; i++){
            sum += cnpj[i] * multipliers[i];
        }

        int mod = sum % 11;

        return mod < 2 ? 0 : 11-mod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CNPJ cnpj1 = (CNPJ) o;
        return Arrays.equals(cnpj, cnpj1.cnpj);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(cnpj);
    }

    @Override
    public String toString() {
        return "CNPJ{" +
                "cnpj=" + Arrays.toString(cnpj) +
                '}';
    }
}
