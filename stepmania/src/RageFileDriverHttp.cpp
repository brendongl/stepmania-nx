#include "global.h"
#include "RageFileDriverHttp.h"
#include "RageFile.h"
#include "RageUtil.h"
#include "RageUtil_FileDB.h"
#include "RageLog.h"

#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>

static const int HTTP_RING_BYTES = 6 * 1024 * 1024;   // ~20 s of a 2.5 Mbps DDR video
static const int HTTP_CONNECT_TIMEOUT_MS = 1000;
static const int HTTP_RECV_TIMEOUT_MS = 1500;          // reader thread: no data for this long = stream dead
static const int HTTP_MANIFEST_TIMEOUT_MS = 2000;      // first fetch at startup
static const int HTTP_REPROBE_TIMEOUT_MS = 600;        // later retries happen on the main thread at song load
static const float OFFLINE_RETRY_SECONDS = 20.0f;
static const float PROBE_AFTER_IDLE_SECONDS = 10.0f;  // before listing videos for a song, re-check a server we have not heard from lately
static const int HTTP_PROBE_TIMEOUT_MS = 400;
static const int READ_BUDGET_OPENING_MS = 1500;        // while ffmpeg probes the file (song load, not gameplay)
static const int READ_BUDGET_PLAYING_MS = 60;          // during playback: never hold the main thread longer
static const int READ_BUDGET_SEEK_MS = 400;            // one-shot after a seek (loop/rewind reconnects)
static const float OPENING_PHASE_SECONDS = 4.0f;

#define NVLOG(...) do { if( LOG ) LOG->Info( "[NetVideo] " __VA_ARGS__ ); } while(0)
#define NVTRACE(...) do { if( LOG ) LOG->Trace( "[NetVideo] " __VA_ARGS__ ); } while(0)

static struct FileDriverEntry_HTTP: public FileDriverEntry
{
	FileDriverEntry_HTTP(): FileDriverEntry( "HTTP" ) { }
	RageFileDriver *Create( const RString &sRoot ) const { return new RageFileDriverHttp( sRoot ); }
} const g_RegisterDriverHttp;

/* Never-expiring FilenameDB filled from the manifest. */
class HttpFilenameDB: public FilenameDB
{
public:
	HttpFilenameDB() { ExpireSeconds = -1; }
};

static RString UrlEncodePath( const RString &sPath )
{
	static const char *hex = "0123456789ABCDEF";
	RString sOut;
	for( unsigned i = 0; i < sPath.size(); ++i )
	{
		unsigned char c = sPath[i];
		if( isalnum(c) || c == '/' || c == '-' || c == '_' || c == '.' || c == '~' )
			sOut += c;
		else
		{
			sOut += '%';
			sOut += hex[c >> 4];
			sOut += hex[c & 15];
		}
	}
	return sOut;
}

static void SetNonBlocking( int fd, bool bOn )
{
	int flags = fcntl( fd, F_GETFL, 0 );
	if( flags < 0 ) flags = 0;
	if( bOn ) flags |= O_NONBLOCK; else flags &= ~O_NONBLOCK;
	fcntl( fd, F_SETFL, flags );
}

/* Blocking GET with timeouts. Returns the connected socket with the response body about to start. */
int RageFileObjHttp::HttpGet( const RString &sHost, int iPort, const RString &sRequestPath, int iRangeStart,
	int iTimeoutMs, int &iStatus, RString &sLeftover, int &iContentLength )
{
	iStatus = 0; iContentLength = -1; sLeftover = "";

	struct addrinfo hints, *res = nullptr;
	memset( &hints, 0, sizeof(hints) );
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	char port[16];
	snprintf( port, sizeof(port), "%d", iPort );
	if( getaddrinfo(sHost.c_str(), port, &hints, &res) != 0 || res == nullptr )
		return -1;

	int fd = socket( res->ai_family, res->ai_socktype, res->ai_protocol );
	if( fd < 0 ) { freeaddrinfo(res); return -1; }

	SetNonBlocking( fd, true );
	int rc = connect( fd, res->ai_addr, res->ai_addrlen );
	freeaddrinfo( res );
	if( rc < 0 && errno != EINPROGRESS )
	{
		close( fd );
		return -1;
	}
	if( rc < 0 )
	{
		struct pollfd p = { fd, POLLOUT, 0 };
		if( poll(&p, 1, iTimeoutMs) <= 0 ) { close(fd); return -1; }
		int err = 0; socklen_t len = sizeof(err);
		if( getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0 ) { close(fd); return -1; }
	}
	SetNonBlocking( fd, false );
	int one = 1;
	setsockopt( fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one) );

	RString sReq = ssprintf( "GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: StepMania-NetVideo/1\r\nConnection: close\r\n",
		sRequestPath.c_str(), sHost.c_str() );
	if( iRangeStart > 0 )
		sReq += ssprintf( "Range: bytes=%d-\r\n", iRangeStart );
	sReq += "\r\n";

	size_t sent = 0;
	while( sent < sReq.size() )
	{
		struct pollfd p = { fd, POLLOUT, 0 };
		if( poll(&p, 1, iTimeoutMs) <= 0 ) { close(fd); return -1; }
		ssize_t n = send( fd, sReq.data() + sent, sReq.size() - sent, 0 );
		if( n <= 0 ) { close(fd); return -1; }
		sent += n;
	}

	/* Read headers. */
	RString sHdr;
	char buf[4096];
	while( true )
	{
		struct pollfd p = { fd, POLLIN, 0 };
		if( poll(&p, 1, iTimeoutMs) <= 0 ) { close(fd); return -1; }
		ssize_t n = recv( fd, buf, sizeof(buf), 0 );
		if( n <= 0 ) { close(fd); return -1; }
		sHdr.append( buf, n );
		size_t end = sHdr.find( "\r\n\r\n" );
		if( end != RString::npos )
		{
			sLeftover = sHdr.substr( end + 4 );
			sHdr.erase( end );
			break;
		}
		if( sHdr.size() > 64*1024 ) { close(fd); return -1; }
	}

	if( sHdr.size() < 12 || sHdr.compare(0, 5, "HTTP/") != 0 )
	{
		close( fd );
		return -1;
	}
	iStatus = atoi( sHdr.c_str() + 9 );

	vector<RString> asLines;
	split( sHdr, "\r\n", asLines );
	for( unsigned i = 1; i < asLines.size(); ++i )
	{
		RString l = asLines[i];
		l.MakeLower();
		if( l.compare(0, 15, "content-length:") == 0 )
			iContentLength = atoi( l.c_str() + 15 );
	}
	return fd;
}

/* --- driver --- */

RageFileDriverHttp::RageFileDriverHttp( const RString &sUrl ):
	RageFileDriver( new HttpFilenameDB ), m_iPort(80), m_sBasePath("/"), m_bOnline(false), m_bManifestLoaded(false),
	m_iFiles(0), m_bOffline(false), m_StateMutex( "NetVideoState" )
{
	RString s = sUrl;
	if( s.compare(0, 7, "http://") == 0 )
		s = s.substr( 7 );
	size_t slash = s.find( '/' );
	RString sHostPort = slash == RString::npos ? s : s.substr( 0, slash );
	m_sBasePath = slash == RString::npos ? RString("/") : s.substr( slash );
	if( m_sBasePath.empty() || m_sBasePath[m_sBasePath.size()-1] != '/' )
		m_sBasePath += "/";
	size_t colon = sHostPort.find( ':' );
	if( colon != RString::npos )
	{
		m_sHost = sHostPort.substr( 0, colon );
		m_iPort = atoi( sHostPort.c_str() + colon + 1 );
	}
	else
		m_sHost = sHostPort;

	m_LastAttempt.Touch();
	m_bOnline = LoadManifest( HTTP_MANIFEST_TIMEOUT_MS );
	m_bManifestLoaded = m_bOnline;
	if( m_bOnline ) m_LastOK.Touch();
	NVLOG( "server %s:%d%s -> %s, %d files", m_sHost.c_str(), m_iPort, m_sBasePath.c_str(),
		m_bOnline ? "online" : "OFFLINE (videos hidden, retrying every 20 s)", m_iFiles );
}

bool RageFileDriverHttp::Available()
{
	LockMut( m_StateMutex );
	if( m_bOnline && !m_bOffline )
	{
		if( m_LastOK.Ago() < PROBE_AFTER_IDLE_SECONDS )
			return true;
		/* Quiet for a while (song select): make sure the server is still there before a song picks a
		 * network video, so a dead server gives the static background instead of a black movie. */
		int iStatus, iLen;
		RString sLeftover;
		int fd = RageFileObjHttp::HttpGet( m_sHost, m_iPort, m_sBasePath + "manifest.txt", 0, HTTP_PROBE_TIMEOUT_MS, iStatus, sLeftover, iLen );
		if( fd >= 0 ) close( fd );
		if( fd >= 0 && iStatus == 200 )
		{
			m_LastOK.Touch();
			return true;
		}
		m_bOffline = true;
		m_LastAttempt.Touch();
		NVLOG( "probe failed (status %d): hiding videos for %d s", iStatus, (int) OFFLINE_RETRY_SECONDS );
		return false;
	}
	if( m_LastAttempt.Ago() < OFFLINE_RETRY_SECONDS )
		return false;
	m_LastAttempt.Touch();
	/* Re-probe: fetch the manifest again (cheap, and it also picks up new files on the server). */
	int iFilesBefore = m_iFiles;
	if( LoadManifest(HTTP_REPROBE_TIMEOUT_MS) )
	{
		m_bOnline = true;
		m_bOffline = false;
		m_LastOK.Touch();
		NVLOG( "server is back (%d files, %d new)", m_iFiles, m_iFiles - iFilesBefore );
		return true;
	}
	return false;
}

void RageFileDriverHttp::NoteSuccess()
{
	LockMut( m_StateMutex );
	m_LastOK.Touch();
}

void RageFileDriverHttp::NoteFailure( const char *szWhy )
{
	LockMut( m_StateMutex );
	if( !m_bOffline )
	{
		m_bOffline = true;
		m_OfflineSince.Touch();
		m_LastAttempt.Touch();
		NVLOG( "marking server offline for %d s (%s); songs use their static background meanwhile", (int) OFFLINE_RETRY_SECONDS, szWhy );
	}
}

void RageFileDriverHttp::GetDirListing( const RString &sPath, vector<RString> &asAddTo, bool bOnlyDirs, bool bReturnPathToo )
{
	if( !Available() ) return;
	RageFileDriver::GetDirListing( sPath, asAddTo, bOnlyDirs, bReturnPathToo );
}

RageFileManager::FileType RageFileDriverHttp::GetFileType( const RString &sPath )
{
	if( !Available() ) return RageFileManager::TYPE_NONE;
	return RageFileDriver::GetFileType( sPath );
}

int RageFileDriverHttp::GetFileSizeInBytes( const RString &sFilePath )
{
	if( !Available() ) return -1;
	return RageFileDriver::GetFileSizeInBytes( sFilePath );
}

RageFileDriverHttp::~RageFileDriverHttp()
{
}

bool RageFileDriverHttp::LoadManifest( int iTimeoutMs )
{
	int iStatus, iLen;
	RString sBody;
	int fd = RageFileObjHttp::HttpGet( m_sHost, m_iPort, m_sBasePath + "manifest.txt", 0, iTimeoutMs, iStatus, sBody, iLen );
	if( fd < 0 || iStatus != 200 )
	{
		if( fd >= 0 ) close( fd );
		NVLOG( "manifest.txt unavailable (status %d)", iStatus );
		return false;
	}
	char buf[8192];
	while( true )
	{
		struct pollfd p = { fd, POLLIN, 0 };
		if( poll(&p, 1, iTimeoutMs) <= 0 ) break;
		ssize_t n = recv( fd, buf, sizeof(buf), 0 );
		if( n <= 0 ) break;
		sBody.append( buf, n );
		if( sBody.size() > 8*1024*1024 ) break;
	}
	close( fd );
	if( iLen > 0 && (int) sBody.size() < iLen )
	{
		NVLOG( "manifest.txt truncated (%d of %d bytes)", (int) sBody.size(), iLen );
		return false;
	}
	int iAdded = 0;

	vector<RString> asLines;
	split( sBody, "\n", asLines );
	for( unsigned i = 0; i < asLines.size(); ++i )
	{
		RString l = asLines[i];
		TrimRight( l, "\r" );
		size_t tab = l.find( '\t' );
		if( tab == RString::npos ) continue;
		int iSize = atoi( l.substr(0, tab).c_str() );
		RString sPath = l.substr( tab + 1 );
		if( sPath.empty() || iSize < 0 ) continue;
		if( sPath[0] != '/' ) sPath = "/" + sPath;
		if( FDB->GetFileType(sPath) == RageFileManager::TYPE_FILE ) continue; // already known from an earlier load
		FDB->AddFile( sPath, iSize, 0, nullptr );
		++iAdded;
	}
	m_iFiles += iAdded;
	return true;
}

RageFileBasic *RageFileDriverHttp::Open( const RString &sPath_, int iMode, int &iError )
{
	if( iMode & RageFile::WRITE )
	{
		iError = EROFS;
		return nullptr;
	}
	if( !Available() )
	{
		iError = ENOENT;
		return nullptr;
	}
	RString sPath = sPath_;
	FDB->ResolvePath( sPath );
	if( FDB->GetFileType(sPath) != RageFileManager::TYPE_FILE )
	{
		iError = ENOENT;
		return nullptr;
	}
	int iSize = FDB->GetFileSize( sPath );

	/* Open the stream here, on the caller's thread: if the server is gone we fail the open (ENOENT)
	 * and the caller never gets a half-working file. A failure inside ffmpeg's probe is the one path
	 * that has crashed in testing, so do not let it happen for a server that is already down. */
	RString sRel = sPath;
	if( !sRel.empty() && sRel[0] == '/' ) sRel = sRel.substr( 1 );
	RString sRequestPath = UrlEncodePath( m_sBasePath + sRel );
	int iStatus, iLen;
	RString sLeftover;
	int fd = RageFileObjHttp::HttpGet( m_sHost, m_iPort, sRequestPath, 0, HTTP_CONNECT_TIMEOUT_MS, iStatus, sLeftover, iLen );
	if( fd < 0 || iStatus != 200 )
	{
		if( fd >= 0 ) close( fd );
		NoteFailure( fd < 0 ? "open: connect failed" : "open: bad status" );
		NVLOG( "open %s failed (status %d); reported as missing", sPath.c_str(), iStatus );
		iError = ENOENT;
		return nullptr;
	}
	NoteSuccess();
	return new RageFileObjHttp( this, sPath, iSize, fd, sLeftover );
}

/* --- file object --- */

RageFileObjHttp::RageFileObjHttp( RageFileDriverHttp *pDriver, const RString &sPath, int iSize, int iSock, const RString &sLeftover ):
	m_pDriver( pDriver ), m_sHost( pDriver->m_sHost ), m_iPort( pDriver->m_iPort ), m_iSize( iSize ),
	m_iStreamPos(0), m_iBufLen(0), m_iPos(0), m_iRestartPos(-1), m_bQuit(false),
	m_bStreamError(false), m_bStreamEOF(false), m_iSock(iSock),
	m_Event( "NetVideo" ), m_iExtraBudgetMs( 0 )
{
	RString sRel = sPath;
	if( !sRel.empty() && sRel[0] == '/' ) sRel = sRel.substr( 1 );
	m_sRequestPath = UrlEncodePath( pDriver->m_sBasePath + sRel );
	m_sDisplayPath = "http://" + m_sHost + ":" + ssprintf("%d", m_iPort) + pDriver->m_sBasePath + sRel;
	m_Buf.resize( HTTP_RING_BYTES );
	/* Body bytes that arrived with the headers of the stream the driver opened for us. */
	const int cap = (int) m_Buf.size();
	for( unsigned i = 0; i < sLeftover.size() && m_iBufLen < cap; ++i )
	{
		m_Buf[ m_iStreamPos % cap ] = sLeftover[i];
		++m_iStreamPos; ++m_iBufLen;
	}
	if( m_iSize <= 0 ) m_bStreamEOF = true;
	m_OpenTimer.Touch();
	m_Thread.SetName( "NetVideo reader" );
	m_Thread.Create( ReaderThread_Start, this );
	NVTRACE( "open %s (%d bytes)", m_sDisplayPath.c_str(), m_iSize );
}

RageFileObjHttp::~RageFileObjHttp()
{
	Close();
}

void RageFileObjHttp::Close()
{
	m_Event.Lock();
	m_bQuit = true;
	if( m_iSock >= 0 )
		shutdown( m_iSock, SHUT_RDWR );
	m_Event.Broadcast();
	m_Event.Unlock();
	if( m_Thread.IsCreated() )
		m_Thread.Wait();
	if( m_iSock >= 0 )
	{
		close( m_iSock );
		m_iSock = -1;
	}
}

int RageFileObjHttp::ReaderThread_Start( void *p )
{
	((RageFileObjHttp *) p)->ReaderThread();
	return 0;
}

/* Reader thread: keep the ring buffer ahead of the consumer, reconnecting when asked to seek. */
void RageFileObjHttp::ReaderThread()
{
	const int cap = (int) m_Buf.size();
	char tmp[32 * 1024];
	while( true )
	{
		int iStartAt = -1;
		m_Event.Lock();
		while( !m_bQuit && m_iRestartPos < 0 && (m_iSock < 0 || m_bStreamError || m_bStreamEOF) )
			m_Event.Wait(); // nothing to do until a seek asks for a new stream
		if( m_bQuit ) { m_Event.Unlock(); return; }
		if( m_iRestartPos >= 0 )
		{
			iStartAt = m_iRestartPos;
			m_iRestartPos = -1;
			m_bStreamError = false;
			m_bStreamEOF = false;
			if( m_iSock >= 0 ) { close( m_iSock ); m_iSock = -1; }
			m_iStreamPos = iStartAt;
			m_iBufLen = 0;
		}
		m_Event.Unlock();

		if( iStartAt >= 0 )
		{
			if( iStartAt >= m_iSize )
			{
				m_Event.Lock(); m_bStreamEOF = true; m_Event.Broadcast(); m_Event.Unlock();
				continue;
			}
			int iStatus, iLen;
			RString sLeftover;
			int fd = HttpGet( m_sHost, m_iPort, m_sRequestPath, iStartAt, HTTP_CONNECT_TIMEOUT_MS, iStatus, sLeftover, iLen );
			bool bOK = fd >= 0 && (iStatus == 206 || (iStatus == 200 && iStartAt == 0));
			m_Event.Lock();
			if( m_bQuit ) { if( fd >= 0 ) close(fd); m_Event.Unlock(); return; }
			if( m_iRestartPos >= 0 ) { if( fd >= 0 ) close(fd); m_Event.Unlock(); continue; } // superseded by a newer seek
			if( !bOK )
			{
				if( fd >= 0 ) close( fd );
				m_bStreamError = true;
				NVLOG( "stream open failed at %d (status %d) for %s", iStartAt, iStatus, m_sDisplayPath.c_str() );
				if( fd < 0 ) m_pDriver->NoteFailure( "connect failed" );
				m_Event.Broadcast();
				m_Event.Unlock();
				continue;
			}
			m_iSock = fd;
			/* Store body bytes that arrived with the headers. */
			for( unsigned i = 0; i < sLeftover.size() && m_iBufLen < cap; ++i )
			{
				m_Buf[ m_iStreamPos % cap ] = sLeftover[i];
				++m_iStreamPos; ++m_iBufLen;
			}
			m_Event.Broadcast();
			m_Event.Unlock();
		}

		/* Stream loop: wait for room, poll the socket, append. */
		m_Event.Lock();
		while( !m_bQuit && m_iRestartPos < 0 && !m_bStreamError && !m_bStreamEOF )
		{
			int iAhead = m_iStreamPos - m_iPos; // bytes buffered ahead of the consumer
			if( iAhead >= cap - (int) sizeof(tmp) )
			{
				m_Event.Wait(); // buffer full; consumer will signal when it reads
				continue;
			}
			int fd = m_iSock;
			int iWant = min( (int) sizeof(tmp), cap - (int) sizeof(tmp) - iAhead );
			if( iWant <= 0 ) { m_Event.Wait(); continue; }
			m_Event.Unlock();

			struct pollfd p = { fd, POLLIN, 0 };
			int pr = poll( &p, 1, HTTP_RECV_TIMEOUT_MS );
			ssize_t n = -1;
			if( pr > 0 )
				n = recv( fd, tmp, iWant, 0 );

			m_Event.Lock();
			if( m_bQuit || m_iRestartPos >= 0 )
				break;
			if( pr == 0 )
			{
				m_bStreamError = true;
				NVLOG( "no data for %d ms at %d, giving up on %s", HTTP_RECV_TIMEOUT_MS, m_iStreamPos, m_sDisplayPath.c_str() );
				m_pDriver->NoteFailure( "stream timeout" );
			}
			else if( n < 0 )
			{
				m_bStreamError = true;
				NVLOG( "recv error %d at %d for %s", errno, m_iStreamPos, m_sDisplayPath.c_str() );
				m_pDriver->NoteFailure( "recv error" );
			}
			else if( n == 0 )
			{
				if( m_iStreamPos >= m_iSize )
					m_bStreamEOF = true;
				else
				{
					m_bStreamError = true;
					NVLOG( "server closed at %d of %d for %s", m_iStreamPos, m_iSize, m_sDisplayPath.c_str() );
					m_pDriver->NoteFailure( "server closed connection" );
				}
			}
			else
			{
				if( (m_iStreamPos & 0xFFFFF) < n ) m_pDriver->NoteSuccess(); // about once per MiB
				for( ssize_t i = 0; i < n; ++i )
					m_Buf[ (m_iStreamPos + i) % cap ] = tmp[i];
				m_iStreamPos += n;
				m_iBufLen = min( m_iBufLen + (int) n, cap );
				if( m_iStreamPos >= m_iSize )
					m_bStreamEOF = true;
			}
			m_Event.Broadcast();
		}
		if( m_iSock >= 0 && (m_bStreamError || m_bStreamEOF || m_iRestartPos >= 0) )
		{
			close( m_iSock );
			m_iSock = -1;
		}
		m_Event.Unlock();
	}
}

void RageFileObjHttp::RestartStream( int iPos )
{
	/* Caller holds m_Event. */
	m_iRestartPos = iPos;
	if( m_iSock >= 0 )
		shutdown( m_iSock, SHUT_RDWR ); // wake a blocked poll/recv
	m_Event.Broadcast();
}

int RageFileObjHttp::SeekInternal( int iOffset )
{
	if( iOffset < 0 ) iOffset = 0;
	if( iOffset > m_iSize ) iOffset = m_iSize;
	m_Event.Lock();
	m_iPos = iOffset;
	const int iWindowStart = m_iStreamPos - m_iBufLen;
	const bool bBuffered = iOffset >= iWindowStart && iOffset <= m_iStreamPos && !m_bStreamError;
	if( !bBuffered )
	{
		RestartStream( iOffset );
		m_iExtraBudgetMs = READ_BUDGET_SEEK_MS;
	}
	m_Event.Unlock();
	return iOffset;
}

int RageFileObjHttp::ReadInternal( void *pBuffer, size_t iBytes )
{
	if( iBytes == 0 ) return 0;
	const int cap = (int) m_Buf.size();
	int iBudget = m_OpenTimer.Ago() < OPENING_PHASE_SECONDS ? READ_BUDGET_OPENING_MS : READ_BUDGET_PLAYING_MS;

	m_Event.Lock();
	iBudget += m_iExtraBudgetMs;
	m_iExtraBudgetMs = 0;
	RageTimer start;
	while( true )
	{
		if( m_iPos >= m_iSize )
		{
			m_Event.Unlock();
			return 0; // EOF
		}
		const int iWindowStart = m_iStreamPos - m_iBufLen;
		if( m_iPos < iWindowStart && m_iRestartPos < 0 )
		{
			/* Behind the window (should only happen after an unusual seek): restart there. */
			RestartStream( m_iPos );
		}
		int iAvail = m_iStreamPos - m_iPos;
		if( iAvail > 0 && m_iPos >= iWindowStart && m_iRestartPos < 0 )
		{
			int n = min( (int) iBytes, iAvail );
			for( int i = 0; i < n; ++i )
				((char *) pBuffer)[i] = m_Buf[ (m_iPos + i) % cap ];
			m_iPos += n;
			m_Event.Broadcast(); // room for the reader
			m_Event.Unlock();
			return n;
		}
		if( m_bStreamError && m_iRestartPos < 0 )
		{
			m_Event.Unlock();
			SetError( "network stream lost" );
			return -1;
		}
		float fElapsedMs = start.Ago() * 1000.0f;
		if( fElapsedMs >= iBudget )
		{
			/* Don't hold the caller (gameplay) any longer: cancel this video. */
			m_bStreamError = true;
			NVLOG( "read of %u at %d waited %d ms with %d buffered; cancelling %s", (unsigned) iBytes, m_iPos, (int) fElapsedMs,
				max( 0, m_iStreamPos - m_iPos ), m_sDisplayPath.c_str() );
			if( m_iSock >= 0 ) shutdown( m_iSock, SHUT_RDWR );
			m_Event.Broadcast();
			m_Event.Unlock();
			m_pDriver->NoteFailure( "read stalled" );
			SetError( "network stream stalled" );
			return -1;
		}
		RageTimer t;
		t.Touch();
		t += (iBudget - fElapsedMs) / 1000.0f;
		m_Event.Wait( &t );
	}
}

/*
 * (c) 2026 Sip n Play / Akumara. Same license terms as StepMania (MIT-style, see RageFileDriverDirect.cpp).
 */
