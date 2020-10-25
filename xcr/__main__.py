import logging, sys
import mmap, os
import struct
from collections import namedtuple
import operator

from . import XCRFile, log

def autoMMap(file,):
	return mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_WRITE if file.writable() else mmap.ACCESS_READ,)

def index(args,):
	with open(args.File, 'rb',) as file, autoMMap(file,) as mm, XCRFile(mm, args.entry_limit,) as theFile:
		# fsck
		for x in theFile:
			pass
		print(theFile)

def extract(args,):
	with open(args.File, 'rb',) as file, autoMMap(file,) as mm, XCRFile(mm, args.entry_limit,) as theFile:
		if args.out != '-':
			raise TypeError("""Not yet implemented.""")
		sys.stdout.buffer.write(theFile._mm[args.offset : args.offset + args.length])

def replace(args,):
	srcDataStats = os.stat(args.in1)
	if args.length is None:
		args.length = srcDataStats.st_size
	
	if srcDataStats.st_size != args.length:
		log.error(f"File size {srcDataStats.st_size} doesn't match segment length {args.length} !")
		return
	with open(args.File, 'r+b') as f, open(args.in1, 'rb') as srcData:
		mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ,)
		mm[args.offset : args.offset+args.length] = srcData.read()
		log.info(f"Wrote {args.length} bytes.")
		mm.flush()


def compare(args,):
	with open(args.File, 'rb') as f, open(args.in1, 'rb') as cmpData:
		mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ,)
		if mm[args.offset : args.offset+args.length] == cmpData.read():
			log.info("Match: OK")
		else:
			log.info("Doesn't match!")

def detailsOfOffset(args,):
	with open(args.File, 'rb') as f:
		mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
		log.info(mm.find(args.offset.to_bytes(length=4, byteorder='little', signed=False)))

def main(args,):
	logging.basicConfig(stream=sys.stderr, level=getattr(logging, args.loglevel))
	getattr(sys.modules[__name__], args.Operation)(args)


if __name__=="__main__":
	def auto_int(x,):
		return int(x, 0)
	import argparse
	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--loglevel", help="""What operation to perform on the file.""", default='DEBUG',)
	parser.add_argument("File", help="""File that should be edited.""", default='-',)
	parser.add_argument("--inplace", help="""Write changes to original file.""", action='store_true', default=True,)
	parser.add_argument("--out", help="""Where to write single item data to. '-' for STDOUT.""", default='-',)
	parser.add_argument("--in1", help="""Where to take data from that should replace an existing entry.""",)
	parser.add_argument("Operation", help="""What operation to perform on the file.""", choices=['index', 'extract', 'replace', 'compare', 'detailsOfOffset',],)
	parser.add_argument("--offset", type=auto_int, help="""Offset to use for single item, if not using paths and filenames.""",)
	parser.add_argument("--length", type=auto_int, help="""Size for the single item, if not using paths and filenames.""",)
	parser.add_argument("--entry_limit", type=auto_int, help="""Refuse from scanning files with more than {{entry_limit}} entries. Guard against DoS attacks.""", default=0x00010000,)
	main(parser.parse_args())
