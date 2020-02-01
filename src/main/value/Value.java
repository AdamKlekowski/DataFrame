package main.value;

public abstract class Value {
    Object innerValue;

    public abstract String toString();
    public abstract Value add(Value v);
    public abstract Value sub(Value v);
    public abstract Value mul(Value v);
    public abstract Value div(Value v);
    public abstract Value pow(Value v);
    public abstract boolean eq(Value v);
    public abstract boolean lte(Value v);
    public abstract boolean gte(Value v);
    public abstract boolean neq(Value v);
    public abstract boolean equals(Value other);
    public abstract int hashCode();
    public abstract Value create(String s);
}
