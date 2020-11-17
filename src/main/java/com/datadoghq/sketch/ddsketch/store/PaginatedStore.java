package com.datadoghq.sketch.ddsketch.store;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class PaginatedStore implements Store {

    private static final int GROWTH = 8;
    private static final int PAGE_SIZE = 128;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final int PAGE_SHIFT = Integer.bitCount(PAGE_MASK);

    private double[][] pages = null;
    private int pageOffset;

    public PaginatedStore() {
        this(0);
    }

    PaginatedStore(int pageOffset) {
        this.pageOffset = pageOffset;
    }

    PaginatedStore(PaginatedStore store) {
        this(store.pageOffset);
        this.pages = deepCopy(store.pages);
    }

    @Override
    public boolean isEmpty() {
        // won't initialise any pages unless until a value is added,
        // and values can't be removed.
        return null == pages;
    }

    @Override
    public int getMinIndex() {
        if (null != pages) {
            for (int i = 0; i < pages.length; ++i) {
                if (null != pages[i]) {
                    for (int j = 0; j < pages[i].length; ++j) {
                        if (pages[i][j] != 0D) {
                            return ((i - pageOffset) << PAGE_SHIFT) + j;
                        }
                    }
                }
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public int getMaxIndex() {
        if (null != pages) {
            for (int i = pages.length - 1; i >= 0; --i) {
                if (null != pages[i]) {
                    for (int j = pages[i].length - 1; j >= 0; --j) {
                        if (pages[i][j] != 0D) {
                            return ((i - pageOffset) << PAGE_SHIFT) + j;
                        }
                    }
                }
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public void add(int index, double count) {
        if (count > 0) {
            int alignedIndex = alignedIndex(index);
            double[] page = getPage(alignedIndex >>> PAGE_SHIFT);
            page[alignedIndex & PAGE_MASK] += count;
        }
    }

    private double[] getPage(int pageIndex) {
        double[] page = pages[pageIndex];
        if (null == page) {
            page = pages[pageIndex] = new double[PAGE_SIZE];
        }
        return page;
    }

    private int alignedIndex(int index) {
        int pageIndex = index < 0
                ? -(-index >>> PAGE_SHIFT) - 1
                : index >>> PAGE_SHIFT;
        if (null == pages) {
            lazyInit(pageIndex);
        } else if (pageIndex + pageOffset < 0) {
            growBelow(pageIndex);
        } else if (pageIndex + pageOffset >= pages.length - 1) {
            growAbove(pageIndex);
        }
        return index + (pageOffset << PAGE_SHIFT);
    }

    private void lazyInit(int pageIndex) {
        pageOffset = -pageIndex;
        pages = new double[GROWTH][];
    }

    private void growBelow(int pageIndex) {
        int requiredExtension = -pageOffset - pageIndex;
        // check if there is space to shift into
        boolean canShiftRight = true;
        for (int i = 0; i < requiredExtension && canShiftRight; ++i) {
            canShiftRight = null == pages[pages.length - i - 1];
        }
        if (canShiftRight) {
            System.arraycopy(pages, 0, pages, requiredExtension, pages.length - requiredExtension);
        } else {
            double[][] newPages = new double[pages.length + aligned(GROWTH, requiredExtension)][];
            System.arraycopy(pages, 0, newPages, requiredExtension, pages.length);
            this.pages = newPages;
        }
        Arrays.fill(pages, 0, requiredExtension, null);
        this.pageOffset = -pageIndex;
    }

    private void growAbove(int pageIndex) {
        this.pages = Arrays.copyOf(pages, pages.length + aligned(GROWTH,pageIndex + 1 + pageOffset));
    }

    @Override
    public Store copy() {
        return new PaginatedStore(this);
    }

    @Override
    public Iterator<Bin> getAscendingIterator() {
        return new AscendingIterator();
    }

    @Override
    public Iterator<Bin> getDescendingIterator() {
        return new DescendingIterator();
    }

    private static int aligned(int alignment, int required) {
        return (required + alignment - 1) & -alignment;
    }

    private static double[][] deepCopy(double[][] pages) {
        if (null != pages) {
            double[][] copy = new double[pages.length][];
            for (int i = 0; i < pages.length; ++i) {
                double[] page = pages[i];
                if (null != page) {
                    copy[i] = Arrays.copyOf(page, page.length);
                }
            }
            return copy;
        }
        return null;
    }

    private final class AscendingIterator implements Iterator<Bin> {

        int pageIndex = 0;
        int valueIndex = 0;
        double[] page = null;
        double next = Double.NaN;

        private AscendingIterator() {
            if (null != pages) {
                for (int i = 0; i < pages.length; ++i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        next = nextInPage();
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !Double.isNaN(next);
        }

        @Override
        public Bin next() {
            double value = next;
            int index = ((pageIndex - pageOffset) << PAGE_SHIFT) + valueIndex;
            ++valueIndex;
            next = nextInPage();
            if (Double.isNaN(next)) {
                for (int i = pageIndex + 1; i < pages.length; ++i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        valueIndex = 0;
                        next = nextInPage();
                        break;
                    }
                }
            }
            return new Bin(index, value);
        }

        private double nextInPage() {
            for (int i = valueIndex; i < page.length; ++i) {
                if (page[i] != 0D) {
                    valueIndex = i;
                    return page[i];
                }
            }
            return Double.NaN;
        }
    }

    private final class DescendingIterator implements Iterator<Bin> {

        int pageIndex = 0;
        int valueIndex = PAGE_SIZE - 1;
        double[] page = null;
        double previous = Double.NaN;

        private DescendingIterator() {
            if (null != pages) {
                for (int i = pages.length - 1; i >= 0; --i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        previous = previousInPage();
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !Double.isNaN(previous);
        }

        @Override
        public Bin next() {
            double value = previous;
            int index = ((pageIndex - pageOffset) << PAGE_SHIFT) + valueIndex;
            --valueIndex;
            previous = previousInPage();
            if (Double.isNaN(previous)) {
                for (int i = pageIndex - 1; i >= 0; --i) {
                    if (pages[i] != null) {
                        page = pages[i];
                        pageIndex = i;
                        valueIndex = page.length - 1;
                        previous = previousInPage();
                        break;
                    }
                }
            }
            return new Bin(index, value);
        }

        private double previousInPage() {
            for (int i = valueIndex; i >= 0; --i) {
                if (page[i] != 0D) {
                    valueIndex = i;
                    return page[i];
                }
            }
            return Double.NaN;
        }
    }
}
