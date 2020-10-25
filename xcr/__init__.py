import logging, sys
import mmap, os
import struct
from collections import namedtuple
import operator

log = logging.getLogger(__name__,)

class XCRFile():
	struct_header = struct.Struct('<16s4sII',)
	struct_entry = struct.Struct('<256s256sII12s',)
	# As described there: http://wiki.xentax.com/index.php/Warlords_XCR
	Entry = namedtuple('Entry', ["fileName", "directoryName", "offset", "length", "zeroX", "index"],)
	def __init__(self, file, entry_limit=0x10000, start=0):
		"""
		
		entry_limit: Relevant for opening files only. May be exceeded by add_entry
		"""
		self.i = start
		self._mm = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_WRITE if file.writable() else mmap.ACCESS_READ,)
		self.magic_leftaligned, self.zero1, number_of_files, self.file_length = XCRFile.struct_header.unpack_from(self._mm, 0,)
		if number_of_files > entry_limit:
			raise f"header states {number_of_files} entries, allowed are {self.entry_limit} entries max."
		self._entries = [None] * number_of_files
		self._entries_initialized = False
	def __enter__(self):
		return self
	def _unpack_entry_at(self, i):
		return XCRFile.Entry._make((*XCRFile.struct_entry.unpack_from(self._mm, XCRFile.struct_header.size + i * XCRFile.struct_entry.size,), i,),)
	def _after_full_load(self):
		"""check if offset of first entry is not adjacent to XCRFAT"""
		# index_of_min = min(range(len(self)), key=lambda i: self[i].offset)
		firstEntry = min(self._entries, key=operator.attrgetter("offset"))
		ueber = firstEntry.offset - (XCRFile.struct_header.size + len(self) * XCRFile.struct_entry.size)
		if ueber > 0:
			if all(elem == 0 for elem in self._mm[firstEntry.offset : XCRFile.struct_header.size + len(self) * XCRFile.struct_entry.size]):
				pass
			else:
				log.warn("""Unsupported "hidden" data area between XCRFAT and first data segment detected!""")
		elif ueber < 0:
			log.error("Archive is - most likely - damaged! Partly unsupported but reserved areas seem to be used by data segments.")
		if self._mm.size() > self.file_length:
			log.warn(f"""Please check Archive Length {self.file_length} vs. File Size {self._mm.size()}, XCRFAT size {len(self) * XCRFile.struct_entry.size} ...""")
		self._entries_initialized = True
	def __len__(self):
		return len(self._entries)
	def __getitem__(self, i):
		ret = self._entries[i]
		if ret is None:
			ret = self._unpack_entry_at(i)
			self._entries[i] = ret
			if not self._entries_initialized and i == len(self):
				# check if all entries have been loaded
				if None not in self._entries:
					self._after_full_load()
		return ret
	def updateXCRFAT(self, entry):
		XCRFile.struct_entry.pack_into(self._mm, XCRFile.struct_header.size + entry.index * XCRFile.struct_entry.size, entry.fileName, entry.directoryName, entry.offset, entry.length, entry.zeroX,)
	def updateHeader(self):
		XCRFile.struct_header.pack_into(self._mm, 0, self.magic_leftaligned, self.zero1, len(self._entries), self.file_length,)
	def append(self, entry):
		"""
		XCRFile.Entry must be initialized with everything except offset and data - entry.offset will be set to target offset, entry.index will be set to list index of entry.
		entry.length may grow anytime before #append is called again - see rewriteLastEntrysLength.
		If entry.length is not known before, at first set it to a number that will not exceed the final data segment length, e.g. 1.
		Alternative naming: add_entry()
		"""
		if entry.length < 0:
			raise ValueError()
		if not self._entries_initialized:
			# need to have everything initialized to know the minimum offset with confidence.
			for i, val in enumerate(self._entries):
				if val == None:
					self._entries[i] = self._unpack_entry_at(i)
			self._after_full_load()
		while True:
			# remap first entry (offset-wise) if offset would collide with XCRFAT (kind of a File Allocation Table)
			firsti = min(range(len(self)), key=lambda i: self[i].offset)
			firstEntry = self._entries[firsti]
			if firstEntry.offset < XCRFile.struct_header.size + (len(self)+1) * XCRFile.struct_entry.size:
				log.info("""Moving first entry's data to new region at end of file and rewriting its entry's offset.""")
				newOffset = self.file_length
				self.file_length += firstEntry.length
				self._mm.resize(self.file_length)
				self._mm.move(newOffset, firstEntry.offset, firstEntry.length)
				firstEntry.offset = newOffset
				self.updateXCRFAT(firstEntry)
			else:
				break

		entry.index = len(self)
		self._entries.append(entry)
		entry.offset = self.file_length
		self.file_length += entry.length
		
		self._mm.resize(self.file_length)
		self.updateXCRFAT(entry)

		self.updateHeader()
	def rewriteLastEntrysLength(self,):
		if not self._entries_initialized:
			raise ValueError()
		entry = self._entries[-1]
		self.file_length -= entry.length
		entry.length = self._mm.size() - self.file_length
		self.file_length = self._mm.size()
		self.updateXCRFAT(entry)
		
		self.updateHeader()
	def flush(self,):
		self._mm.flush()
	def __exit__(self, exc_type, exc_value, traceback):
		self._mm.close()
		if exc_type is not None:
				return False
		return True
	def __str__(self):
		return f"{self.magic_leftaligned}\n{self.zero1}\n{len(self)}\n{self.file_length}\n{self._entries}"
