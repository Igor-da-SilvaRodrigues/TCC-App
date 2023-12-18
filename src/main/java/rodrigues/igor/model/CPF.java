package rodrigues.igor.model;

import java.util.Arrays;
import java.util.Random;

public class CPF {
    private int[] cpf;

    public String getAsString(){
        StringBuilder builder = new StringBuilder();
        for (int i : cpf){
            builder.append(i);
        }
        return builder.toString();
    }

    public CPF() {
        cpf = new int[11];
        Random random = new Random();

        //generate the first 9 digits randomly
        for (int i = 0; i < 9; i++){
            cpf[i] = random.nextInt(10);
        }

        int first = calculateFirstDigit(cpf);
        cpf[9] = first;

        int second = calculateSecondDigit(cpf);
        cpf[10] = second;
    }

    private int calculateSecondDigit(int[] cpf) {
        if(cpf.length < 10){
            throw new RuntimeException("Not enough digits to calculate.");
        }

        int multiplier = 2;
        int sum = 0;
        for (int i = 9; i >= 0; i--, multiplier++){
            sum += multiplier * cpf[i];
        }

        int mod = sum % 11;
        return mod < 2 ? 0 : 11-mod;
    }

    private int calculateFirstDigit(int[] cpf) {
        if(cpf.length < 9){
            throw new RuntimeException("Not enough digits to calculate.");
        }

        int multiplier = 2;
        int sum = 0;
        for (int i = 8; i >= 0; i--, multiplier++){
            sum += multiplier * cpf[i];
        }

        int mod = sum % 11;
        return mod < 2 ? 0 : 11-mod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CPF cpf1 = (CPF) o;
        return Arrays.equals(cpf, cpf1.cpf);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(cpf);
    }

    @Override
    public String toString() {
        return "CPF{" +
                "cpf=" + Arrays.toString(cpf) +
                '}';
    }
}
