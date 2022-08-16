using System;
using System.Collections.Generic;
using System.Text;
using Grpc.Core;
using MagicOnion.Internal;
using MessagePack;

namespace MagicOnion
{
    public static class GrpcMethodHelper
    {
        public sealed class MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>
        {
            public Method<TRawRequest, TRawResponse> Method { get; }

            public TRawRequest ToRawRequest(TRequest value) => BoxHelperCache<TRequest, TRawRequest>.ToRaw(value);
            public TRawResponse ToRawResponse(TResponse value) => BoxHelperCache<TResponse, TRawResponse>.ToRaw(value);
            public TRequest FromRawRequest(TRawRequest value) => BoxHelperCache<TRequest, TRawRequest>.FromRaw(value);
            public TResponse FromRawResponse(TRawResponse value) => BoxHelperCache<TResponse, TRawResponse>.FromRaw(value);

            public MagicOnionMethod(Method<TRawRequest, TRawResponse> method)
            {
                Method = method;
            }
        }
        
        static class BoxHelperCache<T, TRaw>
        {
            public static Func<T, TRaw> ToRaw { get; }
            public static Func<TRaw, T> FromRaw { get; }

            static BoxHelperCache()
            {
                ToRaw = (typeof(TRaw) == typeof(Box<T>))
                    ? (Func<T, TRaw>)(x => (TRaw)(object)Box.Create(x))
                    : x => (TRaw)(object)x;

                FromRaw = (typeof(TRaw) == typeof(Box<T>)
                    ? (Func<TRaw, T>)(x => ((Box<T>)(object)x).Value)
                    : x => (T)(object)x);
            }
        }

        public static MagicOnionMethod<Nil, TResponse, Box<Nil>, TRawResponse> CreateMethod<TResponse, TRawResponse>(MethodType methodType, string serviceName, string name, MessagePackSerializerOptions serializerOptions)
        {
            // WORKAROUND: Prior to MagicOnion 5.0, the request type for the parameter-less method was byte[].
            //             DynamicClient sends byte[], but GeneratedClient sends Nil, which is incompatible,
            //             so as a special case we do not serialize/deserialize and always convert to a fixed values.
            var isMethodResponseTypeBoxed = typeof(TResponse).IsValueType;

            if (isMethodResponseTypeBoxed)
            {
                return new MagicOnionMethod<Nil, TResponse, Box<Nil>, TRawResponse>(new Method<Box<Nil>, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    IgnoreNilMarshaller,
                    (Marshaller<TRawResponse>)(object)CreateBoxedMarshaller<TResponse>(serializerOptions)
                ));
            }
            else
            {
                return new MagicOnionMethod<Nil, TResponse, Box<Nil>, TRawResponse>(new Method<Box<Nil>, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    IgnoreNilMarshaller,
                    (Marshaller<TRawResponse>)(object)CreateMarshaller<TResponse>(serializerOptions)
                ));
            }
        }

        public static MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse> CreateMethod<TRequest, TResponse, TRawRequest, TRawResponse>(MethodType methodType, string serviceName, string name, MessagePackSerializerOptions serializerOptions)
        {
            var isMethodRequestTypeBoxed = typeof(TRequest).IsValueType;
            var isMethodResponseTypeBoxed = typeof(TResponse).IsValueType;

            if (isMethodRequestTypeBoxed && isMethodResponseTypeBoxed)
            {
                return new MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>(new Method<TRawRequest, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    (Marshaller<TRawRequest>)(object)CreateBoxedMarshaller<TRequest>(serializerOptions),
                    (Marshaller<TRawResponse>)(object)CreateBoxedMarshaller<TResponse>(serializerOptions)
                ));
            }
            else if (isMethodRequestTypeBoxed)
            {
                return new MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>(new Method<TRawRequest, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    (Marshaller<TRawRequest>)(object)CreateBoxedMarshaller<TRequest>(serializerOptions),
                    (Marshaller<TRawResponse>)(object)CreateMarshaller<TResponse>(serializerOptions)
                ));
            }
            else if (isMethodResponseTypeBoxed)
            {
                return new MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>(new Method<TRawRequest, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    (Marshaller<TRawRequest>)(object)CreateMarshaller<TRequest>(serializerOptions),
                    (Marshaller<TRawResponse>)(object)CreateBoxedMarshaller<TResponse>(serializerOptions)
                ));
            }
            else
            {
                return new MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>(new Method<TRawRequest, TRawResponse>(
                    methodType,
                    serviceName,
                    name,
                    (Marshaller<TRawRequest>)(object)CreateMarshaller<TRequest>(serializerOptions),
                    (Marshaller<TRawResponse>)(object)CreateMarshaller<TResponse>(serializerOptions)
                ));
            }
        }

        // WORKAROUND: Prior to MagicOnion 5.0, the request type for the parameter-less method was byte[].
        //             DynamicClient sends byte[], but GeneratedClient sends Nil, which is incompatible,
        //             so as a special case we do not serialize/deserialize and always convert to a fixed values.
        public static Marshaller<Box<Nil>> IgnoreNilMarshaller { get; } = new Marshaller<Box<Nil>>(
                serializer: (obj, ctx) =>
                {
                    var writer = ctx.GetBufferWriter();
                    var buffer = writer.GetSpan(MagicOnionMarshallers.UnsafeNilBytes.Length); // Write `Nil` as `byte[]` to the buffer.
                    MagicOnionMarshallers.UnsafeNilBytes.CopyTo(buffer);
                    writer.Advance(buffer.Length);

                    ctx.Complete();
                },
                deserializer: (ctx) => Box.Create(Nil.Default) /* Box.Create always returns cached Box<Nil> */
            );

        private static Marshaller<T> CreateMarshaller<T>(MessagePackSerializerOptions serializerOptions)
            => new Marshaller<T>(
                serializer: (obj, ctx) =>
                {
                    MessagePackSerializer.Serialize(ctx.GetBufferWriter(), obj, serializerOptions);
                    ctx.Complete();
                },
                deserializer: (ctx) => MessagePackSerializer.Deserialize<T>(ctx.PayloadAsReadOnlySequence(), serializerOptions)
            );

        private static Marshaller<Box<T>> CreateBoxedMarshaller<T>(MessagePackSerializerOptions serializerOptions)
            => new Marshaller<Box<T>>(
                serializer: (obj, ctx) =>
                {
                    MessagePackSerializer.Serialize(ctx.GetBufferWriter(), obj.Value, serializerOptions);
                    Box.Return(obj);
                    ctx.Complete();
                },
                deserializer: (ctx) => Box.Create(MessagePackSerializer.Deserialize<T>(ctx.PayloadAsReadOnlySequence(), serializerOptions))
            );
    }
}
