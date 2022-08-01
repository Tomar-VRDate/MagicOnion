using MagicOnion.Utils;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace MagicOnion.Server.Hubs
{
    public class ImmutableArrayGroupRepositoryFactory : IGroupRepositoryFactory
    {
        public IGroupRepository CreateRepository(IMagicOnionLogger logger)
        {
            return new ImmutableArrayGroupRepository(logger);
        }
    }

    public class ImmutableArrayGroupRepository : IGroupRepository
    {
        IMagicOnionLogger logger;

        readonly Func<string, IGroup> factory;
        ConcurrentDictionary<string, IGroup> dictionary = new ConcurrentDictionary<string, IGroup>();

        public ImmutableArrayGroupRepository(IMagicOnionLogger logger)
        {
            this.factory = CreateGroup;
            this.logger = logger;
        }

        public IGroup GetOrAdd(string groupName)
        {
            return dictionary.GetOrAdd(groupName, factory);
        }

        IGroup CreateGroup(string groupName)
        {
            return new ImmutableArrayGroup(groupName, this, logger);
        }

        public bool TryGet(string groupName, [NotNullWhen(true)] out IGroup? group)
        {
            return dictionary.TryGetValue(groupName, out group);
        }

        public bool TryRemove(string groupName)
        {
            return dictionary.TryRemove(groupName, out _);
        }
    }

    public class ImmutableArrayGroup : IGroup
    {
        readonly object gate = new object();
        readonly IGroupRepository parent;
        readonly IMagicOnionLogger logger;

        ImmutableArray<IServiceContextWithResponseStream<StreamingHubResponseMessage>> members;
        IInMemoryStorage? inmemoryStorage;

        public string GroupName { get; }

        public ImmutableArrayGroup(string groupName, IGroupRepository parent, IMagicOnionLogger logger)
        {
            this.GroupName = groupName;
            this.parent = parent;
            this.logger = logger;
            this.members = ImmutableArray<IServiceContextWithResponseStream<StreamingHubResponseMessage>>.Empty;
        }

        public ValueTask<int> GetMemberCountAsync()
        {
            return new ValueTask<int>(members.Length);
        }

        public IInMemoryStorage<T> GetInMemoryStorage<T>()
            where T : class
        {
            lock (gate)
            {
                if (inmemoryStorage == null)
                {
                    inmemoryStorage = new DefaultInMemoryStorage<T>();
                }
                else if (!(inmemoryStorage is IInMemoryStorage<T>))
                {
                    throw new ArgumentException("already initialized inmemory-storage by another type, inmemory-storage only use single type");
                }

                return (IInMemoryStorage<T>)inmemoryStorage;
            }
        }

        public ValueTask AddAsync(ServiceContext context)
        {
            lock (gate)
            {
                members = members.Add((IServiceContextWithResponseStream<StreamingHubResponseMessage>)context);
            }
            return default(ValueTask);
        }

        public ValueTask<bool> RemoveAsync(ServiceContext context)
        {
            lock (gate)
            {
                if (!members.IsEmpty)
                {
                    members = members.Remove((IServiceContextWithResponseStream<StreamingHubResponseMessage>)context);
                    if (inmemoryStorage != null)
                    {
                        inmemoryStorage.Remove(context.ContextId);
                    }

                    if (members.Length == 0)
                    {
                        if (parent.TryRemove(GroupName))
                        {
                            return new ValueTask<bool>(true);
                        }
                    }
                }

                return new ValueTask<bool>(false);
            }
        }

        // broadcast: [methodId, [argument]]

        public Task WriteAllAsync<T>(int methodId, T value, bool fireAndForget)
        {
            var message = new StreamingHubResponseMessage(methodId, value);

            var source = members;

            if (fireAndForget)
            {
                for (int i = 0; i < source.Length; i++)
                {
                    source[i].QueueResponseStreamWrite(message);
                }
                logger.InvokeHubBroadcast(GroupName, source.Length);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteExceptAsync<T>(int methodId, T value, Guid connectionId, bool fireAndForget)
        {
            var message = new StreamingHubResponseMessage(methodId, value);

            var source = members;
            if (fireAndForget)
            {
                var writeCount = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    if (source[i].ContextId != connectionId)
                    {
                        source[i].QueueResponseStreamWrite(message);
                        writeCount++;
                    }
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteExceptAsync<T>(int methodId, T value, Guid[] connectionIds, bool fireAndForget)
        {
            var message = new StreamingHubResponseMessage(methodId, value);

            var source = members;
            if (fireAndForget)
            {
                var writeCount = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    foreach (var item in connectionIds)
                    {
                        if (source[i].ContextId == item)
                        {
                            goto NEXT;
                        }
                    }

                    source[i].QueueResponseStreamWrite(message);
                    writeCount++;
                NEXT:
                    continue;
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteToAsync<T>(int methodId, T value, Guid connectionId, bool fireAndForget)
        {
            var message = new StreamingHubResponseMessage(methodId, value);
            var source = members;

            if (fireAndForget)
            {
                var writeCount = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    if (source[i].ContextId == connectionId)
                    {
                        source[i].QueueResponseStreamWrite(message);
                        writeCount++;
                        break;
                    }
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteToAsync<T>(int methodId, T value, Guid[] connectionIds, bool fireAndForget)
        {
            var message = new StreamingHubResponseMessage(methodId, value);

            var source = members;
            if (fireAndForget)
            {
                var writeCount = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    foreach (var item in connectionIds)
                    {
                        if (source[i].ContextId == item)
                        {
                            source[i].QueueResponseStreamWrite(message);
                            writeCount++;
                            goto NEXT;
                        }
                    }

                NEXT:
                    continue;
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteExceptRawAsync(StreamingHubResponseMessage msg, Guid[] exceptConnectionIds, bool fireAndForget)
        {
            var source = members;
            if (fireAndForget)
            {
                var writeCount = 0;
                if (exceptConnectionIds == null)
                {
                    for (int i = 0; i < source.Length; i++)
                    {
                        source[i].QueueResponseStreamWrite(msg);
                        writeCount++;
                    }
                }
                else
                {
                    for (int i = 0; i < source.Length; i++)
                    {
                        foreach (var item in exceptConnectionIds)
                        {
                            if (source[i].ContextId == item)
                            {
                                goto NEXT;
                            }
                        }
                        source[i].QueueResponseStreamWrite(msg);
                        writeCount++;
                    NEXT:
                        continue;
                    }
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteToRawAsync(StreamingHubResponseMessage msg, Guid[] connectionIds, bool fireAndForget)
        {
            var source = members;
            if (fireAndForget)
            {
                var writeCount = 0;
                if (connectionIds != null)
                {
                    for (int i = 0; i < source.Length; i++)
                    {
                        foreach (var item in connectionIds)
                        {
                            if (source[i].ContextId != item)
                            {
                                goto NEXT;
                            }
                        }
                        source[i].QueueResponseStreamWrite(msg);
                        writeCount++;
                    NEXT:
                        continue;
                    }

                    logger.InvokeHubBroadcast(GroupName, writeCount);
                }
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }
    }
}