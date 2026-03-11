use std::io::Error as IoError;
#[cfg(any(
    target_os = "windows",
    not(any(target_os = "windows", target_os = "linux"))
))]
use std::io::ErrorKind;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::process::Command;

#[cfg(target_os = "windows")]
use std::os::windows::ffi::OsStrExt;
use tracing::debug;
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Com::{COINIT_APARTMENTTHREADED, CoInitializeEx, CoUninitialize};
#[cfg(target_os = "windows")]
use windows_sys::Win32::UI::Shell::{
    Common::ITEMIDLIST, ILCreateFromPathW, ILFree, SHOpenFolderAndSelectItems,
};

pub(super) fn open_in_file_explorer(path: &Path) -> Result<(), IoError> {
    #[cfg(target_os = "windows")]
    {
        let init_result =
            unsafe { CoInitializeEx(std::ptr::null(), COINIT_APARTMENTTHREADED as u32) };
        debug!(
            hresult = init_result,
            "CoInitializeEx for file explorer action"
        );

        let result = open_in_file_explorer_windows(path);

        if init_result >= 0 {
            unsafe {
                CoUninitialize();
            }
        }

        return result;
    }

    #[cfg(target_os = "linux")]
    {
        return open_in_file_explorer_linux(path);
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        let _ = path;
        Err(IoError::new(
            ErrorKind::Unsupported,
            "open in file explorer is not supported on this platform",
        ))
    }
}

#[cfg(target_os = "linux")]
fn open_in_file_explorer_linux(path: &Path) -> Result<(), IoError> {
    let target = if path.is_dir() {
        path
    } else if let Some(parent) = path.parent() {
        parent
    } else {
        path
    };

    debug!(
        destination = %path.display(),
        target = %target.display(),
        exists = path.exists(),
        "opening destination with xdg-open"
    );

    let status = Command::new("xdg-open").arg(target).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(IoError::other(format!(
            "xdg-open failed with status {status}"
        )))
    }
}

#[cfg(target_os = "windows")]
fn open_in_file_explorer_windows(path: &Path) -> Result<(), IoError> {
    let parent = path.parent().map(|value| value.display().to_string());
    debug!(
        destination = %path.display(),
        exists = path.exists(),
        parent = ?parent,
        "resolved destination path for file explorer"
    );

    if path.exists() {
        return select_file_in_explorer(path);
    }

    if let Some(parent) = path.parent() {
        debug!(
            parent = %parent.display(),
            "destination missing; opening parent directory"
        );
        return open_folder_in_explorer(parent);
    }

    debug!("destination has no parent; opening destination path as folder");
    open_folder_in_explorer(path)
}

#[cfg(target_os = "windows")]
fn select_file_in_explorer(path: &Path) -> Result<(), IoError> {
    let Some(parent) = path.parent() else {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "file path has no parent directory",
        ));
    };

    let parent_pidl = create_pidl(parent)?;
    let item_pidl = match create_pidl(path) {
        Ok(value) => value,
        Err(error) => {
            unsafe {
                ILFree(parent_pidl);
            }
            return Err(error);
        }
    };
    let selected_items = [item_pidl as *const ITEMIDLIST];

    let result = unsafe {
        SHOpenFolderAndSelectItems(
            parent_pidl as *const ITEMIDLIST,
            1,
            selected_items.as_ptr(),
            0,
        )
    };
    unsafe {
        ILFree(item_pidl);
        ILFree(parent_pidl);
    }

    debug!(
        destination = %path.display(),
        parent = %parent.display(),
        hresult = result,
        "SHOpenFolderAndSelectItems select result"
    );

    if result >= 0 {
        Ok(())
    } else {
        Err(IoError::other(format!(
            "SHOpenFolderAndSelectItems failed with HRESULT {result:#x}"
        )))
    }
}

#[cfg(target_os = "windows")]
fn open_folder_in_explorer(path: &Path) -> Result<(), IoError> {
    let folder_pidl = create_pidl(path)?;
    let result = unsafe {
        SHOpenFolderAndSelectItems(folder_pidl as *const ITEMIDLIST, 0, std::ptr::null(), 0)
    };
    unsafe {
        ILFree(folder_pidl);
    }

    debug!(
        folder = %path.display(),
        hresult = result,
        "SHOpenFolderAndSelectItems folder result"
    );

    if result >= 0 {
        Ok(())
    } else {
        Err(IoError::other(format!(
            "SHOpenFolderAndSelectItems failed with HRESULT {result:#x}"
        )))
    }
}

#[cfg(target_os = "windows")]
fn create_pidl(path: &Path) -> Result<*mut ITEMIDLIST, IoError> {
    let wide_path = path_to_wide(path);
    let pidl = unsafe { ILCreateFromPathW(wide_path.as_ptr()) };
    if pidl.is_null() {
        Err(IoError::new(
            ErrorKind::NotFound,
            format!("failed to create PIDL for {}", path.display()),
        ))
    } else {
        Ok(pidl)
    }
}

#[cfg(target_os = "windows")]
fn path_to_wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}
