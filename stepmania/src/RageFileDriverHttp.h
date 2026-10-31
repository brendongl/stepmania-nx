/* RageFileDriverHttp - read-only files served by a plain HTTP server (Sip n Play network video).
 *
 * Mount with FILEMAN->Mount( "http", "http://host:port/base/", "/Songs" ). The server must publish
 * "<base>/manifest.txt" (one "size<TAB>relative/path" line per file) and answer Range requests.
 * The file list comes from the manifest only, so a dead server at startup simply means the files
 * do not exist. Reads are served from a ring buffer that a reader thread fills; a read that would
 * block longer than a short budget fails instead, so a network stall cancels the video and never
 * stalls the caller (gameplay). Nothing is ever written. */
#ifndef RAGE_FILE_DRIVER_HTTP_H
#define RAGE_FILE_DRIVER_HTTP_H

#include "RageFileDriver.h"
#include "RageFileBasic.h"
#include "RageThreads.h"
#include "RageTimer.h"
#include <vector>

class RageFileDriverHttp: public RageFileDriver
{
public:
	RageFileDriverHttp( const RString &sUrl );
	virtual ~RageFileDriverHttp();

	virtual RageFileBasic *Open( const RString &sPath, int iMode, int &iError );
	virtual void GetDirListing( const RString &sPath, vector<RString> &asAddTo, bool bOnlyDirs, bool bReturnPathToo );
	virtual RageFileManager::FileType GetFileType( const RString &sPath );
	virtual int GetFileSizeInBytes( const RString &sFilePath );
	virtual void FlushDirCache( const RString & /* sPath */ ) { } // the manifest is the only source of truth
	virtual bool Remount( const RString & /* sPath */ ) { return false; }

	/* True when the server answered recently. While it is down the files are hidden, so songs fall back
	 * to their static background instead of a black movie. Re-probes the manifest every RETRY seconds. */
	bool Available();
	void NoteFailure( const char *szWhy ); // called by file objects when a stream dies
	void NoteSuccess();                    // called whenever the server answered
	bool IsOnline() const { return m_bOnline; }
	int GetFileCount() const { return m_iFiles; }

	RString m_sHost;
	int m_iPort;
	RString m_sBasePath; // "/base/" (always starts and ends with '/')

private:
	bool LoadManifest( int iTimeoutMs );
	bool m_bOnline;       // manifest loaded at least once
	bool m_bManifestLoaded;
	int m_iFiles;
	RageTimer m_LastAttempt;   // last manifest fetch / probe
	RageTimer m_LastOK;        // last time the server answered anything
	RageTimer m_OfflineSince;  // zero when fine
	bool m_bOffline;
	RageMutex m_StateMutex;
};

class RageFileObjHttp: public RageFileObj
{
public:
	/* iSock: an already-connected stream positioned at byte 0 (the driver opens it synchronously so an
	 * unreachable server is reported as a missing file instead of a read error inside ffmpeg). */
	RageFileObjHttp( RageFileDriverHttp *pDriver, const RString &sPath, int iSize, int iSock, const RString &sLeftover );
	virtual ~RageFileObjHttp();

	virtual int GetFileSize() const { return m_iSize; }
	virtual RString GetDisplayPath() const { return m_sDisplayPath; }

	/* Blocking helper used by the driver for the manifest and by the reader thread. Returns the socket
	 * fd positioned at the first body byte (headers consumed; leftover body bytes copied to sLeftover),
	 * or -1. iStatus receives the HTTP status. */
	static int HttpGet( const RString &sHost, int iPort, const RString &sRequestPath, int iRangeStart,
		int iTimeoutMs, int &iStatus, RString &sLeftover, int &iContentLength );

protected:
	virtual int SeekInternal( int iOffset );
	virtual int ReadInternal( void *pBuffer, size_t iBytes );
	virtual int WriteInternal( const void * /* pBuffer */, size_t /* iBytes */ ) { SetError( "read-only" ); return -1; }

private:
	static int ReaderThread_Start( void *p );
	void ReaderThread();
	void RestartStream( int iPos );
	void Close();

	RageFileDriverHttp *m_pDriver; // outlives every file object (mounted for the program's life)
	RString m_sHost;
	int m_iPort;
	RString m_sRequestPath; // URL-encoded, absolute
	RString m_sDisplayPath;
	int m_iSize;

	/* Ring buffer holding file bytes [m_iStreamPos - m_iBufLen, m_iStreamPos). Byte at file offset o
	 * lives at m_Buf[o % capacity]. Guarded by m_Event. */
	std::vector<char> m_Buf;
	int m_iStreamPos;  // next file offset the reader thread will store
	int m_iBufLen;     // valid bytes behind m_iStreamPos
	int m_iPos;        // consumer position

	int m_iRestartPos;  // -1, or a position the reader must reconnect at
	bool m_bQuit;
	bool m_bStreamError; // reader gave up (server gone / timeout); reads past the buffer fail
	bool m_bStreamEOF;
	int m_iSock;

	RageEvent m_Event;
	RageThread m_Thread;
	RageTimer m_OpenTimer;
	int m_iExtraBudgetMs; // one-shot extra wait after a seek
};

#endif
