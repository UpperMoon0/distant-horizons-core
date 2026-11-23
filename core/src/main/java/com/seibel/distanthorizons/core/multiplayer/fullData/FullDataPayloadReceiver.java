package com.seibel.distanthorizons.core.multiplayer.fullData;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.RemovalCause;
import com.seibel.distanthorizons.core.config.Config;
import com.seibel.distanthorizons.core.logging.DhLogger;
import com.seibel.distanthorizons.core.logging.DhLoggerBuilder;
import com.seibel.distanthorizons.core.network.INetworkObject;
import com.seibel.distanthorizons.core.network.messages.fullData.FullDataSplitMessage;
import com.seibel.distanthorizons.core.sql.dto.FullDataSourceV2DTO;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class FullDataPayloadReceiver implements AutoCloseable
{
	private static final DhLogger LOGGER = new DhLoggerBuilder()
			.fileLevelConfig(Config.Common.Logging.logNetworkEventToFile)
			.build();
	
	private final ConcurrentMap<Integer, CompositeByteBuf> buffersById = CacheBuilder.newBuilder()
		.expireAfterAccess(30, TimeUnit.SECONDS)
		// Expired split transfers must release their Netty components. Explicit
		// removals transfer ownership to the response or to decodeDataSource.
		.removalListener((RemovalNotification<Integer, CompositeByteBuf> notification) ->
		{
			CompositeByteBuf buffer = notification.getValue();
			if (notification.getCause() == RemovalCause.EXPIRED && buffer != null && buffer.refCnt() > 0)
			{
				buffer.release();
			}
		})
			.<Integer, CompositeByteBuf>build().asMap();
	
	@Override
	public void close()
	{
		for (CompositeByteBuf buffer : this.buffersById.values())
		{
			if (buffer.refCnt() > 0) { buffer.release(); }
		}
		this.buffersById.clear();
	}
	
	public void receiveChunk(FullDataSplitMessage message)
	{
		this.buffersById.compute(message.bufferId, (bufferId, composite) ->
		{
			if (message.isFirst)
			{
				// A restarted transfer may reuse this key. Discard the previous
				// components before beginning a new buffer.
				if (composite != null && composite.refCnt() > 0) { composite.release(); }
				composite = Unpooled.compositeBuffer();
				LOGGER.debug("Created new full data buffer [" + message.bufferId + "]: [" + composite + "]");
			}
			else if (composite == null)
			{
				LOGGER.debug("Received non-first full data chunk for empty buffer [" + message.bufferId + "]: [" + message.buffer + "].");
				message.buffer.release();
				return null;
			}
			
			message.buffer.readerIndex(0);
			composite.addComponent(message.buffer);
			composite.writerIndex(composite.writerIndex() + message.buffer.writerIndex());
			LOGGER.debug("Updated full data buffer [" + message.bufferId + "]: [" + composite + "].");
			return composite;
		});
	}
	
	/**
	 * Pin a completed full-data buffer to the response on the network thread.
	 * The response's compression callback can be queued for longer than the
	 * split-buffer cache timeout under load. Removing it here transfers ownership
	 * to the response and prevents valid LOD data from expiring in that queue.
	 */
	public void attachBufferToResponse(FullDataPayload payload)
	{
		if (payload != null)
		{
			payload.dtoBuffer = this.buffersById.remove(payload.dtoBufferId);
		}
	}
	
	public FullDataSourceV2DTO decodeDataSource(FullDataPayload payload)
	{
		CompositeByteBuf compositeByteBuffer =
				payload.dtoBuffer instanceof CompositeByteBuf
						? (CompositeByteBuf) payload.dtoBuffer
						: this.buffersById.remove(payload.dtoBufferId);
		Objects.requireNonNull(compositeByteBuffer, "Unable to get a complete buffer for a received payload. Ignore this if it doesn't spam similar errors");
		
		try
		{
			return INetworkObject.decodeToInstance(FullDataSourceV2DTO.CreateEmptyDataSourceForDecoding(), compositeByteBuffer);
		}
		finally
		{
			// The response owns any attached buffer. Once decoded, release all
			// component ByteBufs; the expiring cache must no longer retain them.
			payload.dtoBuffer = null;
			if (compositeByteBuffer.refCnt() > 0) { compositeByteBuffer.release(); }
		}
	}
	
}
