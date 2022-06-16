using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using JwtAuthApp.Shared;
using MagicOnion;
using MagicOnion.Client;
using MagicOnion.Client.Internal;
using MessagePack;

namespace JwtAuthApp.Client
{
    [Ignore]
    class __IAccountService_GeneratedClient__ : MagicOnionClientBase<IAccountService>
    {
        readonly ClientCore core;

        public __IAccountService_GeneratedClient__(CallInvoker callInvoker, IReadOnlyList<IClientFilter> clientFilter, MessagePackSerializerOptions serializerOptions)
            : this(new MagicOnionClientOptions(callInvoker, null, default, clientFilter), serializerOptions)
        {
            this.core = new ClientCore(serializerOptions);
        }

        public __IAccountService_GeneratedClient__(MagicOnionClientOptions options, MessagePackSerializerOptions serializerOptions)
            : base(options)
        {
            //this.core = new ClientCore(serializerOptions);
        }
        private __IAccountService_GeneratedClient__(MagicOnionClientOptions options, ClientCore core)
            : base(options)
        {
            this.core = core;
        }

        protected override MagicOnionClientBase<IAccountService> Clone(MagicOnionClientOptions options)
            => new __IAccountService_GeneratedClient__(options, core);

        public UnaryResult<SignInResponse> SignInAsync(string signInId, string password)
            => core.SignInAsync.Invoke(this, "IAccountService/SignInAsync", new DynamicArgumentTuple<string, string>(signInId, password));
        public UnaryResult<CurrentUserResponse> GetCurrentUserNameAsync()
            => core.GetCurrentUserNameAsync.Invoke(this, "IAccountService/GetCurrentUserNameAsync", MessagePack.Nil.Default);
        public UnaryResult<string> DangerousOperationAsync()
            => core.DangerousOperationAsync.Invoke(this, "IAccountService/DangerousOperationAsync", MessagePack.Nil.Default);


        public static Func<MagicOnionClientOptions, MagicOnionClientBase<IAccountService>> CreateFactory(MessagePackSerializerOptions serializerOptions)
        {
            var core = new ClientCore(serializerOptions);
            return (options) => new __IAccountService_GeneratedClient__(options, core);
        }

        class ClientCore
        {
            public readonly UnaryMethodRawInvoker<DynamicArgumentTuple<string, string>, SignInResponse> SignInAsync;
            public readonly UnaryMethodRawInvoker<MessagePack.Nil, CurrentUserResponse> GetCurrentUserNameAsync;
            public readonly UnaryMethodRawInvoker<MessagePack.Nil, string> DangerousOperationAsync;

            public ClientCore(MessagePackSerializerOptions serializerOptions)
            {
                SignInAsync = UnaryMethodRawInvoker.Create_ValueType_RefType<DynamicArgumentTuple<string, string>, SignInResponse>(nameof(IAccountService), nameof(SignInAsync), serializerOptions);
                GetCurrentUserNameAsync = UnaryMethodRawInvoker.Create_ValueType_RefType<MessagePack.Nil, CurrentUserResponse>(nameof(IAccountService), nameof(GetCurrentUserNameAsync), serializerOptions);
                DangerousOperationAsync = UnaryMethodRawInvoker.Create_ValueType_RefType<MessagePack.Nil, string>(nameof(IAccountService), nameof(DangerousOperationAsync), serializerOptions);
            }
        }
    }
}