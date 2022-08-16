using MagicOnion.Utils;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MagicOnion.Server.Hubs
{
    public class ConcurrentDictionaryGroupRepositoryFactory : IGroupRepositoryFactory
    {
        public IGroupRepository CreateRepository(IMagicOnionLogger logger)
        {
            return new ConcurrentDictionaryGroupRepository(logger);
        }
    }

    public class ConcurrentDictionaryGroupRepository : IGroupRepository
    {
        IMagicOnionLogger logger;

        readonly Func<string, IGroup> factory;
        ConcurrentDictionary<string, IGroup> dictionary = new ConcurrentDictionary<string, IGroup>();

        public ConcurrentDictionaryGroupRepository(IMagicOnionLogger logger)
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
            return new ConcurrentDictionaryGroup(groupName, this, logger);
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


    public class ConcurrentDictionaryGroup : IGroup
    {
        // ConcurrentDictionary.Count is slow, use external counter.
        int approximatelyLength;

        readonly object gate = new object();

        readonly IGroupRepository parent;
        readonly IMagicOnionLogger logger;

        ConcurrentDictionary<Guid, IServiceContextWithResponseStream<StreamingHubResponseMessage>> members;
        IInMemoryStorage? inmemoryStorage;

        public string GroupName { get; }

        public ConcurrentDictionaryGroup(string groupName, IGroupRepository parent, IMagicOnionLogger logger)
        {
            this.GroupName = groupName;
            this.parent = parent;
            this.logger = logger;
            this.members = new ConcurrentDictionary<Guid, IServiceContextWithResponseStream<StreamingHubResponseMessage>>();
        }

        public ValueTask<int> GetMemberCountAsync()
        {
            return new ValueTask<int>(approximatelyLength);
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
            if (members.TryAdd(context.ContextId, (IServiceContextWithResponseStream<StreamingHubResponseMessage>)context))
            {
                Interlocked.Increment(ref approximatelyLength);
            }
            return default(ValueTask);
        }

        public ValueTask<bool> RemoveAsync(ServiceContext context)
        {
            if (members.TryRemove(context.ContextId, out _))
            {
                Interlocked.Decrement(ref approximatelyLength);
                if (inmemoryStorage != null)
                {
                    inmemoryStorage.Remove(context.ContextId);
                }
            }

            if (members.Count == 0)
            {
                if (parent.TryRemove(GroupName))
                {
                    return new ValueTask<bool>(true);
                }
            }
            return new ValueTask<bool>(false);
        }

        // broadcast: [methodId, [argument]]

        public Task WriteAllAsync<T>(int methodId, T value, bool fireAndForget)
        {
            var message = StreamingHubResponseMessage.Create(methodId, value);

            if (fireAndForget)
            {
                var writeCount = 0;
                foreach (var item in members)
                {
                    item.Value.QueueResponseStreamWrite(message);
                    writeCount++;
                }
                logger.InvokeHubBroadcast(GroupName, writeCount);
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteExceptAsync<T>(int methodId, T value, Guid connectionId, bool fireAndForget)
        {
            var message = StreamingHubResponseMessage.Create(methodId, value);
            if (fireAndForget)
            {
                var writeCount = 0;
                foreach (var item in members)
                {
                    if (item.Value.ContextId != connectionId)
                    {
                        item.Value.QueueResponseStreamWrite(message);
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
            var message = StreamingHubResponseMessage.Create(methodId, value);
            if (fireAndForget)
            {
                var writeCount = 0;
                foreach (var item in members)
                {
                    foreach (var item2 in connectionIds)
                    {
                        if (item.Value.ContextId == item2)
                        {
                            goto NEXT;
                        }
                    }
                    item.Value.QueueResponseStreamWrite(message);
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
            var message = StreamingHubResponseMessage.Create(methodId, value);
            if (fireAndForget)
            {
                if (members.TryGetValue(connectionId, out var context))
                {
                    context.QueueResponseStreamWrite(message);
                    logger.InvokeHubBroadcast(GroupName, 1);
                }
                return TaskEx.CompletedTask;
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }

        public Task WriteToAsync<T>(int methodId, T value, Guid[] connectionIds, bool fireAndForget)
        {
            var message = StreamingHubResponseMessage.Create(methodId, value);
            if (fireAndForget)
            {
                var writeCount = 0;
                foreach (var item in connectionIds)
                {
                    if (members.TryGetValue(item, out var context))
                    {
                        context.QueueResponseStreamWrite(message);
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

        public Task WriteExceptRawAsync(StreamingHubResponseMessage msg, Guid[] exceptConnectionIds, bool fireAndForget)
        {
            // oh, copy is bad but current gRPC interface only accepts byte[]...
            if (fireAndForget)
            {
                if (exceptConnectionIds == null)
                {
                    var writeCount = 0;
                    foreach (var item in members)
                    {
                        item.Value.QueueResponseStreamWrite(msg);
                        writeCount++;
                    }
                    logger.InvokeHubBroadcast(GroupName, writeCount);
                    return TaskEx.CompletedTask;
                }
                else
                {
                    var writeCount = 0;
                    foreach (var item in members)
                    {
                        foreach (var item2 in exceptConnectionIds)
                        {
                            if (item.Value.ContextId == item2)
                            {
                                goto NEXT;
                            }
                        }
                        item.Value.QueueResponseStreamWrite(msg);
                        writeCount++;
                        NEXT:
                        continue;
                    }
                    logger.InvokeHubBroadcast(GroupName, writeCount);
                    return TaskEx.CompletedTask;
                }
            }
            else
            {
                throw new NotSupportedException("The write operation must be called with Fire and Forget option");
            }
        }



        public Task WriteToRawAsync(StreamingHubResponseMessage msg, Guid[] connectionIds, bool fireAndForget)
        {
            if (fireAndForget)
            {
                if (connectionIds != null)
                {
                    var writeCount = 0;
                    foreach (var item in connectionIds)
                    {
                        if (members.TryGetValue(item, out var context))
                        {
                            context.QueueResponseStreamWrite(msg);
                            writeCount++;
                        }
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

        ValueTask ToPromise(ValueTask[] whenAll, int index)
        {
            var promise = new ReservedWhenAllPromise(index);
            for (int i = 0; i < index; i++)
            {
                promise.Add(whenAll[i]);
            }
            return promise.AsValueTask();
        }
    }
}