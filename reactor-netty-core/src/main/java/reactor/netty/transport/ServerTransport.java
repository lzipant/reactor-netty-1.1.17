/*
 * Copyright (c) 2020-2023 VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.DecoderException;
import io.netty.util.AttributeKey;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.netty.ChannelBindException;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.FutureMono;
import reactor.netty.channel.AbortedException;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.internal.util.MapUtils;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.netty.ReactorNetty.format;

/**
 * A generic server {@link Transport} that will {@link #bind()} to a local address and provide a {@link DisposableServer}.
 *
 * @param <T> ServerTransport implementation
 * @param <CONF> Server Configuration implementation
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class ServerTransport<T extends ServerTransport<T, CONF>,
		CONF extends ServerTransportConfig<CONF>>
		extends Transport<T, CONF> {

	/**
	 * Binds the {@link ServerTransport} and returns a {@link Mono} of {@link DisposableServer}. If
	 * {@link Mono} is cancelled, the underlying binding will be aborted. Once the {@link
	 * DisposableServer} has been emitted and is not necessary anymore, disposing the main server
	 * loop must be done by the user via {@link DisposableServer#dispose()}.
	 *
	 * @return a {@link Mono} of {@link DisposableServer}
	 */
	// SNIPPET_START bind
	public Mono<? extends DisposableServer> bind() {
		// 这里的config参考HttpServerBind的无参构造器
		CONF config = configuration();
		Objects.requireNonNull(config.bindAddress(), "bindAddress");

		Mono<? extends DisposableServer> mono =  Mono.create(sink -> {
			// 在HttpServerBind中设置的以及这里获取到的都是InetSocketAddress
			SocketAddress local = Objects.requireNonNull(config.bindAddress().get(), "Bind Address supplier returned null");
			// HttpServerBind中的设置的是InetSocketAddress类型的
			if (local instanceof InetSocketAddress) {
				InetSocketAddress localInet = (InetSocketAddress) local;

				// 如果地址还没解析过则解析
				if (localInet.isUnresolved()) {
					local = AddressUtils.createResolved(localInet.getHostName(), localInet.getPort());
				}
			}

			boolean isDomainSocket = false;
			// 创建disposableServer
			DisposableBind disposableServer;
			// 对于HttpServerBind来说，这里的local是InetSocketAddress类型的，不是DomainSocketAddress
			if (local instanceof DomainSocketAddress) {
				isDomainSocket = true;
				disposableServer = new UdsDisposableBind(sink, config, local);
			}
			else {
				// 创建server
				disposableServer = new InetDisposableBind(sink, config, local);
			}

			/*
			 * 创建连接观察者，config中的childObserver默认是空操作。
			 * 所以在Spring Boot中会往HttpServer中添加一个handler，然后会被封装为ConnectionObserver。
			 */
			ConnectionObserver childObs =
					new ChildObserver(config.defaultChildObserver().then(config.childObserver()));
			/*
			 * 从config中获取eventLoop和channelInitializer来创建acceptor
			 * 该acceptor就是reactor模型中的重要角色，
			 * 这里通过config来获取EventLoopGroup、ChannelHandler、childOptions、childAttrs，
			 * 这些都是Netty中的概念或组件。
			 * 对于EventLoopGroup的获取，内部会通过LoopResources来实现。
			 *
			 * 这里的Acceptor继承了ChannelInboundHandlerAdapter，该类是Netty中的概念。
			 *
			 * 客户端连接到服务端后，Acceptor的channelRead方法会把通道注册到child事件循环中。
			 */
			Acceptor acceptor = new Acceptor(config.childEventLoopGroup(), config.channelInitializer(childObs, null, true),
					config.childOptions, config.childAttrs, isDomainSocket);
			/*
			 * 执行底层的bind操作
			 * 这里的AcceptorInitializer是一个ChannelInitializer，用于初始化channel
			 */
			TransportConnector.bind(config, new AcceptorInitializer(acceptor), local, isDomainSocket)
			                  .subscribe(disposableServer);
		});

		// 这里默认是null，所以可以忽略
		if (config.doOnBind() != null) {
			// 执行bind回调
			mono = mono.doOnSubscribe(s -> config.doOnBind().accept(config));
		}
		return mono;
	}
	// SNIPPET_END bind

	/**
	 * Starts the server in a blocking fashion, and waits for it to finish initializing
	 * or the startup timeout expires (the startup timeout is {@code 45} seconds). The
	 * returned {@link DisposableServer} offers simple server API, including to {@link
	 * DisposableServer#disposeNow()} shut it down in a blocking fashion.
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	// SNIPPET_START bindNow_1
	/*
	 * 在SpringBoot中的NettyWebServer中的startHttpServer方法中调用了该方法
	 */
	public final DisposableServer bindNow() {
		// 设置绑定操作的超时时间为45秒
		return bindNow(Duration.ofSeconds(45));
	}
	// SNIPPET_END bindNow_1

	/**
	 * Start the server in a blocking fashion, and wait for it to finish initializing
	 * or the provided startup timeout expires. The returned {@link DisposableServer}
	 * offers simple server API, including to {@link DisposableServer#disposeNow()}
	 * shut it down in a blocking fashion.
	 *
	 * @param timeout max startup timeout (resolution: ns)
	 * @return a {@link DisposableServer}
	 */
	// SNIPPET_START bindNow_2
	public final DisposableServer bindNow(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		try {
			// 调用bind方法，必须在timeout之内绑定完成
			return Objects.requireNonNull(bind().block(timeout), "aborted");
		}
		catch (IllegalStateException e) {
			if (e.getMessage()
			     .contains("blocking read")) {
				throw new IllegalStateException(getClass().getSimpleName() + " couldn't be started within " + timeout.toMillis() + "ms");
			}
			throw e;
		}
	}
	// SNIPPET_END bindNow_2

	/**
	 * Start the server in a fully blocking fashion, not only waiting for it to initialize
	 * but also blocking during the full lifecycle of the server. Since most
	 * servers will be long-lived, this is more adapted to running a server out of a main
	 * method, only allowing shutdown of the servers through {@code sigkill}.
	 * <p>
	 * Note: {@link Runtime#addShutdownHook(Thread) JVM shutdown hook} is added by
	 * this method in order to properly disconnect the server upon receiving a
	 * {@code sigkill} signal.</p>
	 *
	 * @param timeout a timeout for server shutdown
	 * @param onStart an optional callback on server start
	 */
	public final void bindUntilJavaShutdown(Duration timeout, @Nullable Consumer<DisposableServer> onStart) {
		Objects.requireNonNull(timeout, "timeout");
		DisposableServer facade = Objects.requireNonNull(bindNow(), "facade");

		if (onStart != null) {
			onStart.accept(facade);
		}

		Runtime.getRuntime()
		       .addShutdownHook(new Thread(() -> facade.disposeNow(timeout)));

		facade.onDispose()
		      .block();
	}

	/**
	 * Injects default attribute to the future child {@link Channel} connections. It
	 * will be available via {@link Channel#attr(AttributeKey)}.
	 * If the {@code value} is {@code null}, the attribute of the specified {@code key}
	 * is removed.
	 *
	 * @param key the attribute key
	 * @param value the attribute value - null to remove a key
	 * @param <A> the attribute type
	 * @return a new {@link ServerTransport} reference
	 * @see ServerBootstrap#childAttr(AttributeKey, Object)
	 */
	public <A> T childAttr(AttributeKey<A> key, @Nullable A value) {
		Objects.requireNonNull(key, "key");
		T dup = duplicate();
		dup.configuration().childAttrs = TransportConfig.updateMap(configuration().childAttrs, key, value);
		return dup;
	}

	/**
	 * Set or add the given {@link ConnectionObserver} for each remote connection.
	 *
	 * @param observer the {@link ConnectionObserver} addition
	 * @return a new {@link ServerTransport} reference
	 */
	// SNIPPET_START childObserve
	public T childObserve(ConnectionObserver observer) {
		Objects.requireNonNull(observer, "observer");
		T dup = duplicate();
		// 获取当前已经配置的observer
		ConnectionObserver current = configuration().childObserver;
		// 和已有的observer合并在一起
		dup.configuration().childObserver = current == null ? observer : current.then(observer);
		return dup;
	}
	// SNIPPET_END childObserve

	/**
	 * Injects default options to the future child {@link Channel} connections. It
	 * will be available via {@link Channel#config()}.
	 * If the {@code value} is {@code null}, the attribute of the specified {@code key}
	 * is removed.
	 * Note: Setting {@link ChannelOption#AUTO_READ} option will be ignored. It is configured to be {@code false}.
	 *
	 * @param key the option key
	 * @param value the option value - null to remove a key
	 * @param <A> the option type
	 * @return a new {@link ServerTransport} reference
	 * @see ServerBootstrap#childOption(ChannelOption, Object)
	 */
	@SuppressWarnings("ReferenceEquality")
	public <A> T childOption(ChannelOption<A> key, @Nullable A value) {
		Objects.requireNonNull(key, "key");
		// Reference comparison is deliberate
		if (ChannelOption.AUTO_READ == key) {
			if (value instanceof Boolean && Boolean.TRUE.equals(value)) {
				log.error("ChannelOption.AUTO_READ is configured to be [false], it cannot be set to [true]");
			}
			@SuppressWarnings("unchecked")
			T dup = (T) this;
			return dup;
		}
		T dup = duplicate();
		dup.configuration().childOptions = TransportConfig.updateMap(configuration().childOptions, key, value);
		return dup;
	}

	/**
	 * Set or add a callback called when {@link ServerTransport} is about to start listening for incoming traffic.
	 *
	 * @param doOnBind a consumer observing connected events
	 * @return a new {@link ServerTransport} reference
	 */
	public T doOnBind(Consumer<? super CONF> doOnBind) {
		Objects.requireNonNull(doOnBind, "doOnBind");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<CONF> current = (Consumer<CONF>) configuration().doOnBind;
		dup.configuration().doOnBind = current == null ? doOnBind : current.andThen(doOnBind);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link DisposableServer} has been started.
	 *
	 * @param doOnBound a consumer observing connected events
	 * @return a new {@link ServerTransport} reference
	 */
	public T doOnBound(Consumer<? super DisposableServer> doOnBound) {
		Objects.requireNonNull(doOnBound, "doOnBound");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<DisposableServer> current = (Consumer<DisposableServer>) configuration().doOnBound;
		dup.configuration().doOnBound = current == null ? doOnBound : current.andThen(doOnBound);
		return dup;
	}

	/**
	 * Set or add a callback called on new remote {@link Connection}.
	 *
	 * @param doOnConnection a consumer observing remote connections
	 * @return a new {@link ServerTransport} reference
	 */
	public T doOnConnection(Consumer<? super Connection> doOnConnection) {
		Objects.requireNonNull(doOnConnection, "doOnConnected");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<Connection> current = (Consumer<Connection>) configuration().doOnConnection;
		dup.configuration().doOnConnection = current == null ? doOnConnection : current.andThen(doOnConnection);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link DisposableServer} has been shutdown.
	 *
	 * @param doOnUnbound a consumer observing unbound events
	 * @return a new {@link ServerTransport} reference
	 */
	public T doOnUnbound(Consumer<? super DisposableServer> doOnUnbound) {
		Objects.requireNonNull(doOnUnbound, "doOnUnbound");
		T dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<DisposableServer> current = (Consumer<DisposableServer>) configuration().doOnUnbound;
		dup.configuration().doOnUnbound = current == null ? doOnUnbound : current.andThen(doOnUnbound);
		return dup;
	}

	/**
	 * The host to which this server should bind.
	 *
	 * @param host the host to bind to.
	 * @return a new {@link ServerTransport} reference
	 */
	public T host(String host) {
		Objects.requireNonNull(host, "host");
		return bindAddress(() -> AddressUtils.updateHost(configuration().bindAddress(), host));
	}

	/**
	 * The port to which this server should bind.
	 *
	 * @param port The port to bind to.
	 * @return a new {@link ServerTransport} reference
	 */
	public T port(int port) {
		return bindAddress(() -> AddressUtils.updatePort(configuration().bindAddress(), port));
	}

	/**
	 * Based on the actual configuration, returns a {@link Mono} that triggers:
	 * <ul>
	 *     <li>an initialization of the event loop groups</li>
	 *     <li>loads the necessary native libraries for the transport</li>
	 * </ul>
	 * By default, when method is not used, the {@code bind operation} absorbs the extra time needed to load resources.
	 *
	 * @return a {@link Mono} representing the completion of the warmup
	 * @since 1.0.3
	 */
	public Mono<Void> warmup() {
		return Mono.fromRunnable(() -> {
			// event loop group for the server
			configuration().childEventLoopGroup();

			// event loop group for the server selector
			configuration().eventLoopGroup();
		});
	}

	static final Logger log = Loggers.getLogger(ServerTransport.class);

	static class Acceptor extends ChannelInboundHandlerAdapter {

		final EventLoopGroup childGroup;
		final ChannelHandler childHandler;
		final Map<ChannelOption<?>, ?> childOptions;
		final Map<AttributeKey<?>, ?> childAttrs;
		final boolean isDomainSocket;

		Runnable enableAutoReadTask;

		Acceptor(EventLoopGroup childGroup, ChannelHandler childHandler,
				Map<ChannelOption<?>, ?> childOptions, Map<AttributeKey<?>, ?> childAttrs,
				boolean isDomainSocket) {
			this.childGroup = childGroup;
			this.childHandler = childHandler;
			this.childOptions = childOptions;
			this.childAttrs = childAttrs;
			this.isDomainSocket = isDomainSocket;
		}

		// SNIPPET_START Acceptor_channelRead
		// 该方法会处理客户端连接通道。
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			final Channel child = (Channel) msg;

			/*
			 * 为客户端通道添加handler，这里的childHandler的类型其实是TransportChannelInitializer，是一个通道初始化器。
			 * 这里执行add操作后，该handler被添加到通道的pipeline后，会封装延时任务，等通道注册时会执行这些延时任务。
			 * 所以通道注册时会调用该handler的handlerAdd回调，在该回调中会调用initChannel方法。
			 */
			child.pipeline().addLast(childHandler);

			// 为通道设置选项和属性
			TransportConnector.setChannelOptions(child, childOptions, isDomainSocket);
			TransportConnector.setAttributes(child, childAttrs);

			try {
				/*
				 * 将客户端的通道注册到worker组的事件循环中，
				 * 在register内部会调用next()方法来选择一个事件循环，然后将通道注册到该循环中。
				 */
				childGroup.register(child).addListener((ChannelFutureListener) future -> {
					if (!future.isSuccess()) {
						forceClose(child, future.cause());
					}
				});
			}
			catch (Throwable t) {
				forceClose(child, t);
			}
		}
		// SNIPPET_END Acceptor_channelRead

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			ChannelConfig config = ctx.channel().config();
			if (config.isAutoRead()) {
				// stop accept new connections for 1 second to allow the channel to recover
				// See https://github.com/netty/netty/issues/1328
				config.setAutoRead(false);
				ctx.channel()
				   .eventLoop()
				   .schedule(enableAutoReadTask, 1, TimeUnit.SECONDS)
				   .addListener(future -> {
				       if (!future.isSuccess() && log.isDebugEnabled()) {
				           log.debug(format(ctx.channel(), "Cannot enable auto-read"), future.cause());
				       }
				   });
			}
		}

		void enableAutoReadTask(Channel channel) {

			// Task which is scheduled to re-enable auto-read.
			// It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
			// not be able to load the class because of the file limit it already reached.
			//
			// See https://github.com/netty/netty/issues/1328
			enableAutoReadTask = () -> channel.config().setAutoRead(true);
		}

		static void forceClose(Channel child, Throwable t) {
			child.unsafe().closeForcibly();
			if (log.isWarnEnabled()) {
				log.warn(format(child, "Failed to register an accepted channel: {}"), child, t);
			}
		}
	}

	// SNIPPET_START AcceptorInitializer
	static final class AcceptorInitializer extends ChannelInitializer<Channel> {

		final Acceptor acceptor;

		AcceptorInitializer(Acceptor acceptor) {
			this.acceptor = acceptor;
		}

		// 把acceptor添加到channel的管道末尾
		@Override
		public void initChannel(final Channel ch) {
			ch.eventLoop().execute(() -> ch.pipeline().addLast(acceptor));
		}
	}
	// SNIPPET_END AcceptorInitializer

	static final class ChildObserver implements ConnectionObserver {

		// 组合了其他的连接观察者
		final ConnectionObserver childObs;

		ChildObserver(ConnectionObserver childObs) {
			this.childObs = childObs;
		}

		@Override
		public void onUncaughtException(Connection connection, Throwable error) {
			ChannelOperations<?, ?> ops = ChannelOperations.get(connection.channel());
			if (ops == null && (error instanceof IOException || AbortedException.isConnectionReset(error) ||
					// DecoderException at this point might be SSL handshake issue or other TLS related issue.
					// In case of HTTP if there is an HTTP decoding issue, it is propagated with
					// io.netty.handler.codec.DecoderResultProvider
					// and not with throwing an exception
					error instanceof DecoderException)) {
				if (log.isDebugEnabled()) {
					log.debug(format(connection.channel(), "onUncaughtException(" + connection + ")"), error);
				}
			}
			else {
				log.error(format(connection.channel(), "onUncaughtException(" + connection + ")"), error);
			}
			connection.dispose();
		}

		// SNIPPET_START ChildObserver_onStateChange
		@Override
		public void onStateChange(Connection connection, State newState) {
			if (newState == State.DISCONNECTING) {
				if (connection.channel().isActive() && !connection.isPersistent()) {
					connection.dispose();
				}
			}

			// 处理连接状态
			childObs.onStateChange(connection, newState);
		}
		// SNIPPET_END ChildObserver_onStateChange
	}

	static class DisposableBind implements CoreSubscriber<Channel>, DisposableServer, Connection {

		final MonoSink<DisposableServer> sink;
		final Context                    currentContext;
		final TransportConfig            config;
		final SocketAddress              bindAddress;

		Channel channel;
		Subscription subscription;

		DisposableBind(MonoSink<DisposableServer> sink, TransportConfig config, SocketAddress bindAddress) {
			this.sink = sink;
			this.currentContext = Context.of(sink.contextView());
			this.config = config;
			this.bindAddress = bindAddress;
		}

		@Override
		public Channel channel() {
			return channel;
		}

		@Override
		public Context currentContext() {
			return currentContext;
		}

		@Override
		@SuppressWarnings("FutureReturnValueIgnored")
		public final void dispose() {
			if (channel != null) {
				if (channel.isActive()) {
					//"FutureReturnValueIgnored" this is deliberate
					channel.close();

					LoopResources loopResources = config.loopResources();
					if (loopResources instanceof ConnectionProvider) {
						((ConnectionProvider) loopResources).disposeWhen(bindAddress);
					}
				}
			}
			else {
				subscription.cancel();
			}
		}

		@Override
		@SuppressWarnings("FutureReturnValueIgnored")
		public void disposeNow(Duration timeout) {
			if (isDisposed()) {
				if (log.isDebugEnabled()) {
					log.debug(format(channel(), "Server has been disposed"));
				}
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug(format(channel(), "Server is about to be disposed with timeout: {}"), timeout);
			}
			dispose();
			Mono<Void> terminateSignals = Mono.empty();
			if (config.channelGroup != null && config.channelGroup.size() > 0) {
				HashMap<Channel, List<Mono<Void>>> channelsToMono =
						new HashMap<>(MapUtils.calculateInitialCapacity(config.channelGroup.size()));
				// Wait for the running requests to finish
				for (Channel channel : config.channelGroup) {
					Channel parent = channel.parent();
					// For TCP and HTTP/1.1 the channel parent is the ServerChannel
					boolean isParentServerChannel = parent instanceof ServerChannel;
					List<Mono<Void>> monos =
							MapUtils.computeIfAbsent(channelsToMono,
									isParentServerChannel ? channel : parent,
									key -> new ArrayList<>());
					if (monos.isEmpty() && !isParentServerChannel) {
						// In case of HTTP/2 Reactor Netty will send GO_AWAY with lastStreamId to notify the
						// client to stop opening streams, the actual CLOSE will happen when all
						// streams up to lastStreamId are closed
						monos.add(FutureMono.from(parent.close()));
					}
					ChannelOperations<?, ?> ops = ChannelOperations.get(channel);
					if (ops != null) {
						monos.add(ops.onTerminate().doFinally(sig -> ops.dispose()));
					}
				}
				for (Map.Entry<Channel, List<Mono<Void>>> entry : channelsToMono.entrySet()) {
					Channel channel = entry.getKey();
					List<Mono<Void>> monos = entry.getValue();
					if (monos.isEmpty()) {
						// At this point there are no running requests for this channel
						// This can happen for TCP and HTTP/1.1
						// "FutureReturnValueIgnored" this is deliberate
						channel.close();
					}
					else {
						terminateSignals = Mono.when(monos).and(terminateSignals);
					}
				}
			}

			try {
				onDispose().then(terminateSignals)
				           .block(timeout);
			}
			catch (IllegalStateException e) {
				if (e.getMessage()
						.contains("blocking read")) {
					throw new IllegalStateException("Socket couldn't be stopped within " + timeout.toMillis() + "ms");
				}
				throw e;
			}
		}

		@Override
		public void onComplete() {
		}

		@Override
		public void onError(Throwable t) {
			sink.error(ChannelBindException.fail(bindAddress, t));
		}

		/*
		 * bind操作完了之后会调用该方法，并传入Channel，此时该channel已经完成了bind和listen。
		 * 接下来是不是该accept了？
		 */
		@Override
		public void onNext(Channel channel) {
			this.channel = channel;
			if (log.isDebugEnabled()) {
				log.debug(format(channel, "Bound new server"));
			}
			sink.success(this);
			config.defaultConnectionObserver()
			      .then(config.connectionObserver())
					// 发布状态变更事件
			      .onStateChange(this, ConnectionObserver.State.CONNECTED);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(subscription, s)) {
				this.subscription = s;
				sink.onCancel(this);
				s.request(Long.MAX_VALUE);
			}
		}
	}

	static final class InetDisposableBind extends DisposableBind {

		InetDisposableBind(MonoSink<DisposableServer> sink, TransportConfig config, SocketAddress bindAddress) {
			super(sink, config, bindAddress);
		}

		@Override
		public InetSocketAddress address() {
			return (InetSocketAddress) channel().localAddress();
		}

		@Override
		public String host() {
			return address().getHostString();
		}

		@Override
		public int port() {
			return address().getPort();
		}
	}

	static final class UdsDisposableBind extends DisposableBind {

		UdsDisposableBind(MonoSink<DisposableServer> sink, TransportConfig config, SocketAddress bindAddress) {
			super(sink, config, bindAddress);
		}

		@Override
		public DomainSocketAddress address() {
			return (DomainSocketAddress) channel().localAddress();
		}

		@Override
		public String path() {
			return address().path();
		}
	}
}