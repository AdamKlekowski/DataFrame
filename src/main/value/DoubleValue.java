package main.value;

import java.text.DecimalFormat;

public class DoubleValue extends Value {
    public DoubleValue(Double newValue) {
        innerValue = newValue;
    }
    public DoubleValue() {}

    @Override
    public String toString() {
        DecimalFormat decimal = new DecimalFormat("0.000000000000000000");
        return decimal.format(innerValue);
    }

    @Override
    public DoubleValue add(Value v) {
        return new DoubleValue((Double)innerValue + (Double)v.innerValue);
    }

    @Override
    public Value sub(Value v) {
        return new DoubleValue((Double)innerValue - (Double)v.innerValue);
    }

    @Override
    public Value mul(Value v) {
        return new DoubleValue((Double)innerValue * (Double)v.innerValue);
    }

    @Override
    public Value div(Value v) {
        return new DoubleValue((Double)innerValue / (Double)v.innerValue);
    }

    @Override
    public StringValue pow(Value v) {
        return null;
    }

    @Override
    public boolean eq(Value v) {
        return (Double)innerValue == (Double)v.innerValue;
    }

    @Override
    public boolean lte(Value v) {
        return (Double)innerValue < (Double)v.innerValue;
    }

    @Override
    public boolean gte(Value v) {
        return (Double)innerValue > (Double)v.innerValue;
    }

    @Override
    public boolean neq(Value v) {
        return innerValue != v.innerValue;
    }

    @Override
    public boolean equals(Value other) {
        return innerValue.equals(other.innerValue);
    }

    @Override
    public int hashCode() {
        return innerValue.hashCode();
    }

    @Override
    public DoubleValue create(String s) {
        DoubleValue newValue = new DoubleValue();
        newValue.innerValue = Double.valueOf(s);
        return newValue;
    }
}
