using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Text;
using MessagePack;

namespace MagicOnion.Internal
{
    // Pubternal API
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class Box<T> : IEquatable<Box<T>>
    {
        public readonly T Value;

        internal Box(T value)
        {
            Value = value;
        }

        public bool Equals(Box<T> other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<T>.Default.Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is Box<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<T>.Default.GetHashCode(Value);
        }

        public static bool operator ==(Box<T> valueA, Box<T> valueB)
            => object.ReferenceEquals(valueA, null) ? object.ReferenceEquals(valueB, null) : valueA.Equals(valueB);

        public static bool operator !=(Box<T> valueA, Box<T> valueB)
            => !(valueA == valueB);
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class Box
    {
        private static readonly Box<MessagePack.Nil> Nil = new Box<MessagePack.Nil>(MessagePack.Nil.Default);
        private static readonly Box<bool> BoolTrue = new Box<bool>(true);
        private static readonly Box<bool> BoolFalse = new Box<bool>(false);

        public static Box<T> Create<T>(T value)
            => (value is MessagePack.Nil) ? (Box<T>)(object)Nil
                : (value is bool b) ? (Box<T>)(object)(b ? BoolTrue : BoolFalse)
                : new Box<T>(value);

        public static T FromRaw<T, TRaw>(TRaw rawValue)
            => Cache<T, TRaw>.FromRaw(rawValue);
        public static TRaw ToRaw<T, TRaw>(T value)
            => Cache<T, TRaw>.ToRaw(value);

        static class Cache<T, TRaw>
        {
            public static Func<T, TRaw> ToRaw { get; }
            public static Func<TRaw, T> FromRaw { get; }

            static Cache()
            {
                ToRaw = (typeof(TRaw) == typeof(Box<T>))
                    ? (Func<T, TRaw>)(x => (TRaw)(object)Box.Create(x))
                    : x => (TRaw)(object)x;

                FromRaw = (typeof(TRaw) == typeof(Box<T>)
                    ? (Func<TRaw, T>)(x => ((Box<T>)(object)x).Value)
                    : x => (T)(object)x);
            }
        }
    }
}
