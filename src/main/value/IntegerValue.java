package main.value;

public class IntegerValue extends Value {
    public IntegerValue(Integer newValue) {
        innerValue = newValue;
    }
    public IntegerValue() {}

    @Override
    public String toString() {
        return innerValue.toString();
    }

    @Override
    public IntegerValue add(Value v) {
        return new IntegerValue((Integer)innerValue + (Integer)v.innerValue);
    }

    @Override
    public Value sub(Value v) {
        return new IntegerValue((Integer)innerValue - (Integer)v.innerValue);
    }

    @Override
    public Value mul(Value v) {
        return new IntegerValue((Integer)innerValue * (Integer)v.innerValue);
    }

    @Override
    public Value div(Value v) {
        return new IntegerValue((Integer)innerValue / (Integer)v.innerValue);
    }

    @Override
    public IntegerValue pow(Value v) {
        return null;
    }

    @Override
    public boolean eq(Value v) {
        return (Integer)innerValue == (Integer)v.innerValue;
    }

    @Override
    public boolean lte(Value v) {
        return (Integer)innerValue < (Integer)v.innerValue;
    }

    @Override
    public boolean gte(Value v) {
        return (Integer)innerValue > (Integer)v.innerValue;
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
    public IntegerValue create(String s) {
        IntegerValue newValue = new IntegerValue();
        newValue.innerValue = Integer.valueOf(s);
        return newValue;
    }
}
