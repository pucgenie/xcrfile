import logging, sys
import mmap, os
import struct
from collections import namedtuple
import operator
"""
coding style:
1. re("[^(,]\)",) should match generators only.
2. line width: 127 characters plus line terminator. Comments / triple-double-quoted texts should not be hard-wrapped.
3. simple tabulator indentation. Don't misuse tabs for layout - align using spaces. Tabs define the indentation level ONLY.
"""

log = logging.getLogger(__name__,)

class Entry():
	struct_entry = struct.Struct('<256s256sII12s',)
	def __init__(self, fileName, directoryName, offset, length, zeroX, xcrfile, i,):
		self.fileName, self.fileName1 = fileName.split(b"\x00", 1,)
		self.directoryName, self.directoryName1 = directoryName.split(b"\x00", 1,)
		self.offset = offset
		self.length = length
		self.zeroX = zeroX
		self.xcrfile = xcrfile
		self.index = i
	def updateXCRFAT(self,):
		# pucgenie: Why is there no error message about undefined _mm, XCRFile, calcEntryOffset, ...?
		Entry.struct_entry.pack_into(self.xcrfile._mm, XCRFile.calcEntryOffset(self.index,), self.fileName, self.directoryName,
			self.offset, self.length, self.zeroX,)
	def __repr__(self,):
		# pucgenie: **** my own coding style rules
		return f"""{{'fileName': {repr(
			self.fileName,)}, 'directoryName': {repr(
			self.directoryName,)}, 'offset': {repr(
			self.offset,)}, 'length': {repr(
			self.length,)},}}"""
	
	@staticmethod
	def unpack_entry_from_XCRFAT(xcrfile, i,):
		return Entry(*Entry.struct_entry.unpack_from(xcrfile._mm, XCRFile.calcEntryOffset(i,),), xcrfile, i,)

class XCRFile():
	struct_header = struct.Struct('<20sII',)
	calcEntryOffset = lambda i: XCRFile.struct_header.size + i * Entry.struct_entry.size
	# As described there: http://wiki.xentax.com/index.php/Warlords_XCR
	def __init__(self, mm, entry_limit=0x10000,):
		"""
		
		entry_limit: Relevant for opening files only. May be exceeded by add_entry
		"""
		self._mm = mm
		self.magic, number_of_files, self.file_length = XCRFile.struct_header.unpack_from(self._mm, 0,)
		self.magic, self.magic1 = self.magic.split(b"\x00", 1,)
		if number_of_files > entry_limit:
			raise f"header states {number_of_files} entries, allowed are {self.entry_limit} entries max."
		# pucgenie: I'm very proud of that multi-purpose bitmap in form of a list
		self._entries = [None,] * number_of_files
		self._entries_initialized = False
	def __enter__(self,):
		return self
	def _after_full_load(self,):
		"""check if offset of first entry is not adjacent to XCRFAT"""
		# index_of_min = min(range(len(self,),), key=lambda i: self[i].offset,)
		firstEntry = min(self._entries, key=operator.attrgetter("offset",),)
		ueber = firstEntry.offset - XCRFile.calcEntryOffset(len(self,),)
		if ueber > 0:
			if all(elem == 0 for elem in self._mm[firstEntry.offset : XCRFile.calcEntryOffset(len(self,),)]):
				pass
			else:
				log.warn("""Unsupported "hidden" data area between XCRFAT and first data segment detected!""",)
		elif ueber < 0:
			log.error("""Archive is - most likely - damaged! Partly unsupported but reserved areas seem to be used by data segments."""
				,)
		if self._mm.size() > self.file_length:
			log.warn(f"""Please check Archive Length {self.file_length} vs. File Size {self._mm.size()}, XCRFAT size {
				len(self,) * Entry.struct_entry.size} ...""",)
		self._entries_initialized = True
	def __len__(self,):
		return len(self._entries,)
	def __getitem__(self, i,):
		ret = self._entries[i]
		if ret is None:
			ret = Entry.unpack_entry_from_XCRFAT(self, i,)
			self._entries[i] = ret
			if not self._entries_initialized and i == len(self,):
				# check if all entries have been loaded
				if None not in self._entries:
					self._after_full_load()
		return ret
	def updateHeader(self,):
		XCRFile.struct_header.pack_into(self._mm, 0, self.magic, len(self._entries,), self.file_length,)
	def append(self, entry,):
		"""
		XCRFile.Entry must be initialized with everything except offset and data - entry.offset will be set to target offset, entry.index will be set to list index of entry.
		entry.length may grow anytime before #append is called again - see rewriteLastEntrysLength.
		If entry.length is not known before, at first set it to a number that will not exceed the final data segment length, e.g. 0, or better yet: 1.
		
		Alternative naming: add_entry()
		"""
		if entry.length < 0:
			raise ValueError()
		if not self._entries_initialized:
			# need to have everything initialized to know the minimum offset with confidence.
			for i, val in enumerate(self._entries,):
				if val == None:
					self._entries[i] = Entry.unpack_entry_from_XCRFAT(self, i,)
			self._after_full_load()
		while True:
			# remap first entry (offset-wise,) if offset would collide with XCRFAT (kind of a File Allocation Table,)
			firsti = min(range(len(self,),), key=lambda i: self[i].offset,)
			firstEntry = self._entries[firsti]
			if firstEntry.offset < XCRFile.calcEntryOffset(len(self,)+1,):
				log.info("""Moving first entry's data to new region at end of file and rewriting its entry's offset.""",)
				newOffset = self.file_length
				self.file_length += firstEntry.length
				self._mm.resize(self.file_length,)
				self._mm.move(newOffset, firstEntry.offset, firstEntry.length,)
				# saveguard1
				self._mm[firstEntry.offset : firstEntry.offset + firstEntry.length] = [0,] * firstEntry.length

				firstEntry.offset = newOffset
				firstEntry.updateXCRFAT()
			else:
				break
		entry.offset = self.file_length
		self.file_length += entry.length
		self._mm.resize(self.file_length,)
		entry.updateXCRFAT()
		self.updateHeader()
	def rewriteLastEntrysLength(self,):
		if not self._entries_initialized:
			raise ValueError()
		entry = self._entries[-1]
		self.file_length -= entry.length
		entry.length = self._mm.size() - self.file_length
		self.file_length = self._mm.size()
		entry.updateXCRFAT()
		self.updateHeader()
	def createEntry(self,):
		"""Some kind of factory method."""
		return Entry(None, None, None, 0, b"", self, len(self,),)
	def flush(self,):
		self._mm.flush()
	def __exit__(self, exc_type, exc_value, traceback,):
		self._mm.close()
		if exc_type is not None:
				return False
		return True
	def __repr__(self,):
		return f"{{'magic': {repr(self.magic,)}, 'file_length': {self.file_length}, 'entries': {repr(self._entries,)},}}"
	
	@staticmethod
	def createEmptyXCRFile(mm : mmap, entry_limit = 0x10000,):
		XCRFile.struct_header.pack_into(mm, 0, b"xcr File 1.00", 0, XCRFile.struct_header.size,)
		return XCRFile(mm, entry_limit,)
