package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;

public class CradleManagerMock extends CradleManager {
  @Override
  public CradleStorage createStorage() {
    return null;
  }
}
