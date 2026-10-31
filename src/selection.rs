//! ファイル一覧の複数選択（クリック / Ctrl / Shift）。

#[derive(Default)]
pub struct Selection {
    selected: Vec<bool>,
    anchor: Option<usize>,
}

impl Selection {
    pub fn reset(&mut self, len: usize) {
        self.selected = vec![false; len];
        self.anchor = None;
    }

    pub fn click(&mut self, index: usize, ctrl: bool, shift: bool) {
        if index >= self.selected.len() {
            return;
        }
        if shift {
            let anchor = self.anchor.unwrap_or(index);
            let (start, end) = (anchor.min(index), anchor.max(index));
            for (i, value) in self.selected.iter_mut().enumerate() {
                *value = (start..=end).contains(&i);
            }
            self.anchor = Some(anchor);
        } else if ctrl {
            self.selected[index] = !self.selected[index];
            self.anchor = Some(index);
        } else {
            self.selected.fill(false);
            self.selected[index] = true;
            self.anchor = Some(index);
        }
    }

    pub fn select_all(&mut self) {
        self.selected.fill(true);
    }

    pub fn clear(&mut self) {
        self.selected.fill(false);
        self.anchor = None;
    }

    pub fn is_selected(&self, index: usize) -> bool {
        self.selected.get(index).copied().unwrap_or(false)
    }

    pub fn indices(&self) -> Vec<usize> {
        (0..self.selected.len())
            .filter(|&i| self.selected[i])
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_len(len: usize) -> Selection {
        let mut s = Selection::default();
        s.reset(len);
        s
    }

    #[test]
    fn plain_click_replaces_selection() {
        let mut s = with_len(4);
        s.click(1, false, false);
        s.click(2, false, false);
        assert_eq!(s.indices(), [2]);
    }

    #[test]
    fn ctrl_click_toggles() {
        let mut s = with_len(4);
        s.click(1, true, false);
        s.click(3, true, false);
        assert_eq!(s.indices(), [1, 3]);
        s.click(1, true, false);
        assert_eq!(s.indices(), [3]);
    }

    #[test]
    fn shift_click_selects_range_from_anchor_in_either_direction() {
        let mut s = with_len(6);
        s.click(2, false, false);
        s.click(4, false, true);
        assert_eq!(s.indices(), [2, 3, 4]);
        s.click(0, false, true);
        assert_eq!(s.indices(), [0, 1, 2]);
    }

    #[test]
    fn out_of_range_click_is_ignored() {
        let mut s = with_len(1);
        s.click(5, false, false);
        assert!(s.indices().is_empty());
    }
}
