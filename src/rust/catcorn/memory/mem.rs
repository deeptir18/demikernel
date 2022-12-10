//! This module contains ways to interface with custom memory allocation/registration.
//! How we will exactly achieve that, I am not sure yet.
//! It seems like standard library containers don't allow custom allocators yet.
const PGSHIFT_4KB: usize = 12;
const PGSHIFT_2MB: usize = 21;
const PGSHIFT_1GB: usize = 30;
pub const PGSIZE_4KB: usize = 1 << PGSHIFT_4KB;
pub const PGSIZE_2MB: usize = 1 << PGSHIFT_2MB;
pub const PGSIZE_1GB: usize = 1 << PGSHIFT_1GB;
const PGMASK_4KB: usize = PGSIZE_4KB - 1;
const PGMASK_2MB: usize = PGSIZE_2MB - 1;
const PGMASK_1GB: usize = PGSIZE_1GB - 1;

#[inline]
pub fn pgoff4kb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_4KB
}

#[inline]
pub fn pgoff2mb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_2MB
}

pub fn pgoff1gb(addr: *const u8) -> usize {
    (addr as usize) & PGMASK_1GB
}

#[inline]
pub fn closest_1g_page(addr: *const u8) -> usize {
    let off = pgoff1gb(addr);
    addr as usize - off
}

#[inline]
pub fn closest_4k_page(addr: *const u8) -> usize {
    let off = pgoff4kb(addr);
    addr as usize - off
}

#[inline]
pub fn closest_2mb_page(addr: *const u8) -> usize {
    let off = pgoff2mb(addr);
    addr as usize - off
}
